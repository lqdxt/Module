--[[
Supports environments 'isRealFS' but usually focused in studio.
in Roblox Studio: insert it in ServerScriptService as a ModuleScript named 'TaskQueue' or your namings
local Queue = require(game.ServerScriptService.TaskQueue)
You should know it uses DataStore for 'files'
]]
local Queue = (function()
 local HttpService = game:GetService("HttpService")
 local RunService = game:GetService("RunService")
 local DataStoreService = game:GetService("DataStoreService")
 local Players = game:GetService("Players")

 local MAX_DATASTORE_SIZE = 4000000
 local LOCK_LEASE_TIME = 180
 
 local VirtualFS = {}
 local dirtyFiles = {}
 local FileVersions = {}
 local StudioStore = nil
 local isRealFS = (typeof(writefile) == "function" and typeof(readfile) == "function")

 local autoSaveInterval = 300
 local autoSaveRunning = false

 if not isRealFS and RunService:IsServer() then
  pcall(function()
   StudioStore = DataStoreService:GetDataStore("PlayerDataStore_v4")
  end)
 end

 local function HasDataStoreBudget(requestType)
  if not StudioStore then return false end
  return DataStoreService:GetRequestBudgetForRequestType(requestType) > 0
 end

 local function RequestWithRetry(actionFn, maxRetries)
  maxRetries = maxRetries or 3
  for attempt = 1, maxRetries do
   local ok, result = pcall(actionFn)
   if ok then return true, result end
   if attempt < maxRetries then
    task.wait(2 ^ attempt)
   end
  end
  return false, "DataStore request failed after retries"
 end

 local function AcquireLockAndRead(filePath)
  if not HasDataStoreBudget(Enum.DataStoreRequestType.UpdateAsync) then
   error("DataStore UpdateAsync budget exhausted", 2)
  end

  local finalData = nil
  local success, err = RequestWithRetry(function()
   return StudioStore:UpdateAsync(filePath, function(oldRecord)
    local now = os.time()
    
    if type(oldRecord) ~= "table" or not oldRecord.Version then
     finalData = { Data = oldRecord, Version = 1, Session = { JobId = game.JobId, LastUpdated = now } }
     return finalData
    end

    local session = oldRecord.Session or {}
    
    if session.JobId and session.JobId ~= "" and session.JobId ~= game.JobId then
     if (now - (session.LastUpdated or 0)) < LOCK_LEASE_TIME then
      finalData = "LOCKED"
      return nil
     end
    end

    oldRecord.Session = {
     JobId = game.JobId,
     LastUpdated = now
    }
    finalData = oldRecord
    return oldRecord
   end)
  end)

  if not success then error("Failed to fetch data: " .. tostring(err), 2) end
  if finalData == "LOCKED" then error("Data is actively locked by another server session.", 2) end

  local content = finalData and finalData.Data or nil
  local version = finalData and finalData.Version or 1
  
  VirtualFS[filePath] = content
  FileVersions[filePath] = version
  return content
 end

 local function SaveToStudioStore(filePath, content, releaseLock)
  if not StudioStore or not content then return false end
  
  if #content > MAX_DATASTORE_SIZE then
   warn(string.format("[Queue] Cannot save '%s': Data exceeds 4MB limit!", filePath))
   return false
  end

  local currentVersion = FileVersions[filePath] or 1

  local success = RequestWithRetry(function()
   return StudioStore:UpdateAsync(filePath, function(oldRecord)
    local now = os.time()
    local dsVersion = (type(oldRecord) == "table" and oldRecord.Version) or 0
    local session = (type(oldRecord) == "table" and oldRecord.Session) or {}

    if dsVersion > currentVersion then
     warn(string.format("[Queue] Version conflict on '%s'. DataStore is newer. Aborting save.", filePath))
     return nil 
    end

    if session.JobId and session.JobId ~= game.JobId and session.JobId ~= "" then
     if (now - (session.LastUpdated or 0)) < LOCK_LEASE_TIME then
      warn(string.format("[Queue] Key '%s' is locked by another server. Aborting save.", filePath))
      return nil
     end
    end

    local newVersion = currentVersion + 1
    FileVersions[filePath] = newVersion

    return {
     Data = content,
     Version = newVersion,
     Session = {
      JobId = releaseLock and "" or game.JobId,
      LastUpdated = now
     }
    }
   end)
  end)

  return success
 end

 local function FlushFilePath(filePath, releaseLock)
  if not StudioStore or not dirtyFiles[filePath] then return end
  local content = VirtualFS[filePath]
  if content ~= nil then
   if SaveToStudioStore(filePath, content, releaseLock) then
    dirtyFiles[filePath] = nil
    if releaseLock then
     VirtualFS[filePath] = nil
     FileVersions[filePath] = nil
    end
   end
  end
 end

 local function FlushAllDirtyParallel()
  local pending = 0
  local startTime = os.clock()

  for filePath in pairs(dirtyFiles) do
   pending += 1
   task.spawn(function()
    FlushFilePath(filePath, true)
    pending -= 1
   end)
  end

  while pending > 0 and (os.clock() - startTime) < 25 do
   task.wait(0.1)
  end
  
  if pending > 0 then
   warn("[Queue] BindToClose timeout! " .. tostring(pending) .. " files failed to save in time.")
  end
 end

 local function StartAutoSaveLoop()
  if autoSaveRunning or isRealFS or not RunService:IsServer() then return end
  autoSaveRunning = true

  task.spawn(function()
   while autoSaveRunning do
    task.wait(autoSaveInterval)
    if next(dirtyFiles) then
     for filePath in pairs(dirtyFiles) do
      if not HasDataStoreBudget(Enum.DataStoreRequestType.UpdateAsync) then break end
      FlushFilePath(filePath, false)
      task.wait(1)
     end
    end
   end
  end)
 end

 if not isRealFS and RunService:IsServer() then
  StartAutoSaveLoop()

  Players.PlayerRemoving:Connect(function(player)
   local playerPrefix = "Player_" .. tostring(player.UserId)
   for filePath in pairs(dirtyFiles) do
    if string.find(filePath, playerPrefix, 1, true) then
     task.spawn(function()
      FlushFilePath(filePath, true)
     end)
    end
   end
  end)

  game:BindToClose(function()
   autoSaveRunning = false
   FlushAllDirtyParallel()
  end)
 end

 local write = isRealFS and writefile or function(filePath, content)
  VirtualFS[filePath] = tostring(content or "")
  dirtyFiles[filePath] = true
 end

 local append = (isRealFS and typeof(appendfile) == "function") and appendfile or function(filePath, content)
  VirtualFS[filePath] = (VirtualFS[filePath] or "") .. tostring(content or "")
  dirtyFiles[filePath] = true
 end

 local read = isRealFS and readfile or function(filePath)
  if VirtualFS[filePath] ~= nil then
   return VirtualFS[filePath]
  end

  if StudioStore then
   local content = AcquireLockAndRead(filePath)
   if content then return content end
  end

  error("File not found or locked: " .. tostring(filePath), 2)
 end

 local queue = {}
 local head = 1
 local processing = false
 local paused = false
 local queueDelay = 0
 local onError = nil

 local function WriteChunkedInternal(filePath, content, chunkSize)
  chunkSize = (type(chunkSize) == "number" and chunkSize) or 50000
  write(filePath, "")
  local totalLen = #content
  for i = 1, totalLen, chunkSize do
   local chunk = string.sub(content, i, math.min(i + chunkSize - 1, totalLen))
   append(filePath, chunk)
   task.wait()
  end
 end

 local function process()
  processing = true
  while head <= #queue do
   while paused do task.wait(0.1) end

   local item = queue[head]
   queue[head] = nil
   head += 1

   if item then
    local results = { pcall(item.fn, table.unpack(item.args)) }
    local ok = results[1]

    if ok then
     if item.callback then task.spawn(item.callback, true, table.unpack(results, 2)) end
    else
     local err = results[2]
     if onError then task.spawn(onError, err, item.fn) else warn("[Queue] Execution error: " .. tostring(err)) end
     if item.callback then task.spawn(item.callback, false, err) end
    end
   end
   if head <= #queue and queueDelay > 0 then task.wait(queueDelay) end
  end
  table.clear(queue)
  head = 1
  processing = false
 end

 local function Load(fn, opts)
  opts = type(opts) == "boolean" and { HasFile = opts } or (opts or {})
  local item = { fn = fn, args = opts.Args or {}, callback = opts.Callback }
  if opts.Priority then table.insert(queue, head, item) else table.insert(queue, item) end
  if not processing then task.spawn(process) end
 end

 local function SaveTableChunked(filePath, dataTable, chunkSize, callback)
  if type(chunkSize) == "function" then callback = chunkSize; chunkSize = 50000 end
  Load(function()
   local jsonStr = HttpService:JSONEncode(dataTable)
   if #jsonStr > MAX_DATASTORE_SIZE then error("Data size exceeds 4MB DataStore limit", 2) end
   task.wait()
   WriteChunkedInternal(filePath, jsonStr, chunkSize)
   return true
  end, { Callback = callback })
 end

 local function ReadJsonChunked(filePath, callback)
  Load(function()
   local raw = read(filePath)
   task.wait()
   return HttpService:JSONDecode(raw)
  end, { Callback = callback })
 end

 local function SavePlayerDataChunked(playerOrUserId, dataKey, dataTable, callback)
  local userId = typeof(playerOrUserId) == "Instance" and playerOrUserId.UserId or playerOrUserId
  local key = string.format("Player_%s_%s", tostring(userId), dataKey)
  SaveTableChunked(key, dataTable, 50000, callback)
 end

 local function ReadPlayerDataChunked(playerOrUserId, dataKey, callback)
  local userId = typeof(playerOrUserId) == "Instance" and playerOrUserId.UserId or playerOrUserId
  local key = string.format("Player_%s_%s", tostring(userId), dataKey)
  ReadJsonChunked(key, callback)
 end

 local function SetAutoSaveInterval(seconds)
  if type(seconds) == "number" and seconds > 0 then autoSaveInterval = seconds end
 end

 local self = {}
 self.Load = Load
 self.SaveTableChunked = SaveTableChunked
 self.ReadJsonChunked = ReadJsonChunked
 self.SavePlayerDataChunked = SavePlayerDataChunked
 self.ReadPlayerDataChunked = ReadPlayerDataChunked
 self.SetAutoSaveInterval = SetAutoSaveInterval
 self.Flush = FlushAllDirtyParallel
 self.Pause = function() paused = true end
 self.Resume = function() paused = false end
 self.Clear = function() table.clear(queue) head = 1 end
 self.SetDelay = function(s) queueDelay = s or 0 end
 self.SetErrorHandler = function(fn) onError = fn end
 self.GetQueueLength = function() return (#queue - head + 1) end
 self.IsProcessing = function() return processing end

 return self
end)()

return Queue
