--!strict
--[[
supports environments 'isRealFS'
in Roblox Studio: insert it in ServerScriptService as a ModuleScript named 'Task' or your namings
local Task = require(game.ServerScriptService.Task)
you should know it uses DataStore for 'files'
]]
local HttpService = game:GetService("HttpService")
local RunService = game:GetService("RunService")
local DataStoreService = game:GetService("DataStoreService")
local Players = game:GetService("Players")

export type Callback = (success: boolean, ...any) -> ()
export type ErrorHandler = (err: string, fn: (...any) -> ...any) -> ()

export type TaskOptions = {
 HasFile: boolean?,
 FilePath: string?,
 Args: { any }?,
 Priority: boolean?,
 Callback: Callback?,
}

type QueueItem = {
 fn: (...any) -> ...any,
 args: { any },
 callback: Callback?,
 opts: TaskOptions,
}

type SessionRecord = {
 JobId: string,
 LastUpdated: number,
}

type MainStoreRecord = {
 Data: string?,
 Chunks: number?,
 Version: number,
 Session: SessionRecord,
}

local Task = {}
Task.__index = Task

local DS_CHUNK_SIZE: number = 2000000 
local LOCK_LEASE_TIME: number = 180
local BUDGET_TIMEOUT: number = 30 

local VirtualFS: { [string]: string } = {}
local dirtyFiles: { [string]: boolean } = {}
local activeLocks: { [string]: boolean } = {}
local FileVersions: { [string]: number } = {}
local StudioStore: DataStore? = nil

local env = getfenv()
local isRealFS: boolean = type(env.writefile) == "function" and type(env.readfile) == "function"
local server: boolean = not isRealFS and RunService:IsServer()

local autoSaveInterval: number = 300
local autoSaveRunning: boolean = false
local onError: ErrorHandler? = nil

local queue: { QueueItem } = {}
local processing: boolean = false
local paused: boolean = false
local queueDelay: number = 0

if server then
 pcall(function()
  StudioStore = DataStoreService:GetDataStore("PlayerDataStore_v7")
 end)
end

local function WaitForBudget(requestType: Enum.DataStoreRequestType): boolean
 if not StudioStore then
  return false
 end
 local start = os.clock()
 while DataStoreService:GetRequestBudgetForRequestType(requestType) <= 0 do
  if os.clock() - start > BUDGET_TIMEOUT then
   warn("DataStore budget timeout exceeded for " .. tostring(requestType))
   return false
  end
  task.wait(1)
 end
 return true
end

local function RequestWithRetry<T>(actionFn: () -> T, maxRetries: number?): (boolean, T | string)
 local retries = maxRetries or 3
 for attempt = 1, retries do
  local ok, result = pcall(actionFn)
  if ok then
   return true, result
  end
  if attempt < retries then
   task.wait(2 ^ attempt)
  end
 end
 return false, "DataStore request failed after retries"
end

local function AcquireLockAndRead(filePath: string): string?
 if not StudioStore then
  return nil
 end

 if not WaitForBudget(Enum.DataStoreRequestType.UpdateAsync) then
  error("Failed to acquire UpdateAsync budget.", 2)
 end

 local finalRecord: MainStoreRecord? = nil
 local isLockedByOther = false

 local success, err = RequestWithRetry(function()
  local store = StudioStore :: DataStore
  return store:UpdateAsync(filePath, function(oldRecord: any)
   local now = os.time()

   if oldRecord == nil then
    local newRecord: MainStoreRecord = {
     Data = nil,
     Version = 1,
     Session = { JobId = game.JobId, LastUpdated = now },
    }
    finalRecord = newRecord
    return newRecord
   end

   if type(oldRecord) ~= "table" or not oldRecord.Version then
    local rec: MainStoreRecord = {
     Data = tostring(oldRecord),
     Version = 1,
     Session = { JobId = game.JobId, LastUpdated = now },
    }
    finalRecord = rec
    return rec
   end

   local recordTable = oldRecord :: MainStoreRecord
   local session = recordTable.Session or { JobId = "", LastUpdated = 0 }

   if session.JobId and session.JobId ~= "" and session.JobId ~= game.JobId then
    if (now - (session.LastUpdated or 0)) < LOCK_LEASE_TIME then
     isLockedByOther = true
     return nil
    end
   end

   recordTable.Session = {
    JobId = game.JobId,
    LastUpdated = now,
   }
   finalRecord = recordTable
   return recordTable
  end)
 end)

 if not success then
  error("Failed to fetch data from DataStore: " .. tostring(err), 2)
 end
 if isLockedByOther then
  error("Data actively locked by another server: " .. filePath, 2)
 end

 if not finalRecord then
  return nil
 end

 local fullContent: string? = nil

 if finalRecord.Chunks and finalRecord.Chunks > 0 then
  local chunks: { string } = {}
  local store = StudioStore :: DataStore
  local parity = (finalRecord.Version % 2 == 0) and "A" or "B" 
  
  for i = 1, finalRecord.Chunks do
   if not WaitForBudget(Enum.DataStoreRequestType.GetAsync) then
    error("Failed to acquire GetAsync budget for chunk reading.", 2)
   end
   
   local chunkKey = string.format("%s_chunk_%s_%d", filePath, parity, i)
   local ok, chunkData = RequestWithRetry(function()
    return store:GetAsync(chunkKey)
   end)
   
   if ok and type(chunkData) == "string" then
    table.insert(chunks, chunkData)
   else
    error("Failed to read chunk " .. i .. " for file " .. filePath, 2)
   end
  end
  fullContent = table.concat(chunks, "")
 else
  fullContent = finalRecord.Data
 end

 if fullContent then
  VirtualFS[filePath] = fullContent
 end
 FileVersions[filePath] = finalRecord.Version or 1
 activeLocks[filePath] = true

 return fullContent
end

local function ReleaseLockWithoutSaving(filePath: string): ()
 if not StudioStore then
  return
 end
 if not WaitForBudget(Enum.DataStoreRequestType.UpdateAsync) then return end
 
 RequestWithRetry(function()
  local store = StudioStore :: DataStore
  return store:UpdateAsync(filePath, function(oldRecord: any)
   if type(oldRecord) == "table" and oldRecord.Session and oldRecord.Session.JobId == game.JobId then
    oldRecord.Session.JobId = ""
    return oldRecord
   end
   return nil
  end)
 end)
 activeLocks[filePath] = nil
end

local function SaveToStudioStore(filePath: string, content: string, releaseLock: boolean): boolean
 if not StudioStore or not content then
  return false
 end

 local store = StudioStore :: DataStore
 local currentVersion = FileVersions[filePath]
 local wasAborted = false

 local contentLen = #content
 local numChunks = math.ceil(contentLen / DS_CHUNK_SIZE)
 local targetVersion = (currentVersion or 0) + 1
 local targetParity = (targetVersion % 2 == 0) and "A" or "B"

 if numChunks > 1 then
  for i = 1, numChunks do
   local startIdx = (i - 1) * DS_CHUNK_SIZE + 1
   local endIdx = math.min(i * DS_CHUNK_SIZE, contentLen)
   local chunkData = string.sub(content, startIdx, endIdx)
   local chunkKey = string.format("%s_chunk_%s_%d", filePath, targetParity, i)

   if not WaitForBudget(Enum.DataStoreRequestType.SetAsync) then
    warn("Budget exhausted while writing chunks. Aborting save to protect main key.")
    return false
   end
   
   local chunkSuccess = RequestWithRetry(function()
    store:SetAsync(chunkKey, chunkData)
   end)
   
   if not chunkSuccess then
    warn(string.format("Failed to save chunk %d for key '%s'. Aborting save.", i, filePath))
    return false 
   end
  end
 end

 if not WaitForBudget(Enum.DataStoreRequestType.UpdateAsync) then return false end
 
 local success, result = RequestWithRetry(function()
  return store:UpdateAsync(filePath, function(oldRecord: any)
   local now = os.time()
   local dsVersion = (type(oldRecord) == "table" and oldRecord.Version) or 0
   local session = (type(oldRecord) == "table" and oldRecord.Session) or {}

   local baseVersion = currentVersion or dsVersion

   if dsVersion > baseVersion then
    warn(string.format("Version conflict on '%s'. Aborting save.", filePath))
    wasAborted = true
    return nil
   end

   if session.JobId and session.JobId ~= game.JobId and session.JobId ~= "" then
    if (now - (session.LastUpdated or 0)) < LOCK_LEASE_TIME then
     warn(string.format("Key '%s' is locked by another server. Aborting save.", filePath))
     wasAborted = true
     return nil
    end
   end

   FileVersions[filePath] = targetVersion

   local recordToSave: MainStoreRecord = {
    Data = if numChunks <= 1 then content else nil,
    Chunks = if numChunks > 1 then numChunks else nil,
    Version = targetVersion,
    Session = {
     JobId = releaseLock and "" or game.JobId,
     LastUpdated = now,
    },
   }
   return recordToSave
  end)
 end)

 if success and (wasAborted or result == nil) then
  return false
 end

 return success
end

local function FlushFilePath(filePath: string, releaseLock: boolean): ()
 if dirtyFiles[filePath] then
  local content = VirtualFS[filePath]
  if content ~= nil then
   if SaveToStudioStore(filePath, content, releaseLock) then
    dirtyFiles[filePath] = nil
    if releaseLock then
     VirtualFS[filePath] = nil
     FileVersions[filePath] = nil
     activeLocks[filePath] = nil
    end
   end
  end
 elseif releaseLock and activeLocks[filePath] then
  ReleaseLockWithoutSaving(filePath)
 end
end

local function FlushAllParallel(): ()
 local pending = 0
 local startTime = os.clock()

 local filesToFlush: { [string]: boolean } = {}
 for f in pairs(dirtyFiles) do filesToFlush[f] = true end
 for f in pairs(activeLocks) do filesToFlush[f] = true end

 for filePath in pairs(filesToFlush) do
  pending += 1
  task.spawn(function()
   FlushFilePath(filePath, true)
   pending -= 1
  end)
 end

 while pending > 0 and (os.clock() - startTime) < BUDGET_TIMEOUT do
  task.wait(0.1)
 end
end

local function StartAutoSaveLoop(): ()
 if autoSaveRunning or isRealFS or not RunService:IsServer() then return end
 autoSaveRunning = true
 task.spawn(function()
  while autoSaveRunning do
   task.wait(autoSaveInterval)
   if next(dirtyFiles) then
    for filePath in pairs(dirtyFiles) do
     FlushFilePath(filePath, false)
     task.wait(1)
    end
   end
  end
 end)
end

local write: (filePath: string, content: string?) -> ()
if isRealFS then
 write = env.writefile
else
 write = function(filePath: string, content: string?)
  VirtualFS[filePath] = tostring(content or "")
  dirtyFiles[filePath] = true
 end
end

local append: (filePath: string, content: string?) -> ()
if isRealFS then
 if type(env.appendfile) == "function" then
  append = env.appendfile
 else
  append = function(filePath: string, content: string?)
   local existing = ""
   if type(env.isfile) == "function" and env.isfile(filePath) then
    local ok, res = pcall(env.readfile, filePath)
    if ok then existing = res end
   end
   env.writefile(filePath, existing .. tostring(content or ""))
  end
 end
else
 append = function(filePath: string, content: string?)
  VirtualFS[filePath] = (VirtualFS[filePath] or "") .. tostring(content or "")
  dirtyFiles[filePath] = true
 end
end

local read: (filePath: string) -> string
if isRealFS then
 read = env.readfile
else
 read = function(filePath: string): string
  if VirtualFS[filePath] ~= nil then
   return VirtualFS[filePath]
  end
  if StudioStore then
   local content = AcquireLockAndRead(filePath)
   if content then return content end
  end
  error("File not found or locked: " .. tostring(filePath), 2)
 end
end

local function WriteChunkedInternal(filePath: string, content: string, chunkSize: number?): ()
 local size = chunkSize or 50000
 write(filePath, "")
 local totalLen = #content
 for i = 1, totalLen, size do
  append(filePath, string.sub(content, i, math.min(i + size - 1, totalLen)))
  task.wait()
 end
end

local function process(): ()
 processing = true
 while #queue > 0 do
  while paused do task.wait(0.1) end
  local item = table.remove(queue, 1)

  if item then
   local fileExists = true
   
   if item.opts.HasFile then
    local path = item.opts.FilePath or ""
    if isRealFS and type(env.isfile) == "function" then
     fileExists = env.isfile(path)
    else
     if VirtualFS[path] ~= nil then
      fileExists = true
     else
      local ok, _ = pcall(function()
       return read(path)
      end)
      fileExists = ok
     end
    end
   end

   if fileExists then
    local results = { pcall(item.fn, table.unpack(item.args)) }
    local ok = results[1] :: boolean

    if ok then
     if item.callback then task.spawn(item.callback, true, table.unpack(results, 2)) end
    else
     local err = tostring(results[2])
     if onError then task.spawn(onError, err, item.fn) else warn("Execution error: " .. err) end
     if item.callback then task.spawn(item.callback, false, err) end
    end
   else
    local err = "File does not exist or is locked"
    if onError then task.spawn(onError, err, item.fn) else warn("Execution error: " .. err) end
    if item.callback then task.spawn(item.callback, false, err) end
   end
  end
  
  if #queue > 0 and queueDelay > 0 then task.wait(queueDelay) end
 end
 processing = false
end

if server then
 StartAutoSaveLoop()

 Players.PlayerRemoving:Connect(function(player: Player)
  local playerPrefix = "Player_" .. tostring(player.UserId)
  local filesToRelease: { [string]: boolean } = {}

  for filePath in pairs(dirtyFiles) do
   if string.find(filePath, playerPrefix, 1, true) then filesToRelease[filePath] = true end
  end
  for filePath in pairs(activeLocks) do
   if string.find(filePath, playerPrefix, 1, true) then filesToRelease[filePath] = true end
  end

  for filePath in pairs(filesToRelease) do
   task.spawn(function()
    FlushFilePath(filePath, true)
   end)
  end
 end)

 game:BindToClose(function()
  autoSaveRunning = false
  FlushAllParallel()
 end)
end

function Task:Queue(fn: (...any) -> ...any, opts: (TaskOptions | boolean)?): ()
 local parsedOpts: TaskOptions = if type(opts) == "boolean"
  then { HasFile = opts }
  else (opts or {})

 local item: QueueItem = {
  fn = fn,
  args = parsedOpts.Args or {},
  callback = parsedOpts.Callback,
  opts = parsedOpts,
 }

 if parsedOpts.Priority then
  table.insert(queue, 1, item)
 else
  table.insert(queue, item)
 end

 if not processing then task.spawn(process) end
end

function Task:SaveTableChunked(
 filePath: string,
 dataTable: { [any]: any },
 chunkSize: (number | Callback)?,
 callback: Callback?
): ()
 local actualChunkSize: number = 50000
 local actualCallback: Callback? = callback

 if type(chunkSize) == "function" then
  actualCallback = chunkSize :: Callback
 elseif type(chunkSize) == "number" then
  actualChunkSize = chunkSize
 end

 Task:Queue(function()
  local jsonStr = HttpService:JSONEncode(dataTable)
  task.wait()
  WriteChunkedInternal(filePath, jsonStr, actualChunkSize)
  return true
 end, { Callback = actualCallback, FilePath = filePath }) 
end

function Task:ReadJsonChunked(filePath: string, callback: Callback?): ()
 Task:Queue(function()
  return HttpService:JSONDecode(read(filePath))
 end, { Callback = callback, FilePath = filePath, HasFile = true })
end

function Task:Save(
 playerOrUserId: Player | number,
 dataKey: string,
 dataTable: { [any]: any },
 callback: Callback?
): ()
 local userId = if typeof(playerOrUserId) == "Instance" and playerOrUserId:IsA("Player")
  then (playerOrUserId :: Player).UserId
  else playerOrUserId :: number

 Task:SaveTableChunked(string.format("Player_%s_%s", tostring(userId), dataKey), dataTable, 50000, callback)
end

function Task:ReadPlayerDataChunked(playerOrUserId: Player | number, dataKey: string, callback: Callback?): ()
 local userId = if typeof(playerOrUserId) == "Instance" and playerOrUserId:IsA("Player")
  then (playerOrUserId :: Player).UserId
  else playerOrUserId :: number

 Task:ReadJsonChunked(string.format("Player_%s_%s", tostring(userId), dataKey), callback)
end

function Task.SetAutoSaveInterval(s: number): ()
 if type(s) == "number" and s > 0 then autoSaveInterval = s end
end

function Task.Flush(): ()
 FlushAllParallel()
end

function Task.Pause(): ()
 paused = true
end

function Task.Resume(): ()
 paused = false
end

function Task.Clear(): ()
 table.clear(queue)
end

function Task.SetDelay(s: number): ()
 queueDelay = s or 0
end

function Task.SetErrorHandler(fn: ErrorHandler): ()
 onError = fn
end

function Task.GetQueueLength(): number
 return #queue
end

function Task.IsProcessing(): boolean
 return processing
end

return Task
