--[[
Supports environments 'isRealFS' but usually focused in studio.
Exc = performance, tasks
Studio = saving data, etc
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
	local activeLocks = {}
	local FileVersions = {} 
	local StudioStore = nil
	local isRealFS = (typeof(writefile) == "function" and typeof(readfile) == "function")

	local autoSaveInterval = 300
	local autoSaveRunning = false
	local onError = nil

	if not isRealFS and RunService:IsServer() then
		pcall(function()
			StudioStore = DataStoreService:GetDataStore("PlayerDataStore_v6")
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
				
				if oldRecord == nil then
					finalData = { _IsEmpty = true }
					return nil 
				end

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
		if finalData == "LOCKED" then error("Data actively locked by another server.", 2) end
		if finalData and finalData._IsEmpty then return nil end

		local content = finalData and finalData.Data or nil
		local version = finalData and finalData.Version or 1
		
		VirtualFS[filePath] = content
		FileVersions[filePath] = version
		activeLocks[filePath] = true 
		return content
	end

	local function ReleaseLockWithoutSaving(filePath)
		if not StudioStore then return end
		RequestWithRetry(function()
			return StudioStore:UpdateAsync(filePath, function(oldRecord)
				if type(oldRecord) == "table" and oldRecord.Session and oldRecord.Session.JobId == game.JobId then
					oldRecord.Session.JobId = ""
					return oldRecord
				end
				return nil
			end)
		end)
		activeLocks[filePath] = nil
	end

	local function SaveToStudioStore(filePath, content, releaseLock)
		if not StudioStore or not content then return false end
		
		if #content > MAX_DATASTORE_SIZE then
			warn(string.format("Cannot save '%s': Exceeds 4MB.", filePath))
			return false
		end

		local currentVersion = FileVersions[filePath]
		local wasAborted = false 

		local success, result = RequestWithRetry(function()
			return StudioStore:UpdateAsync(filePath, function(oldRecord)
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

				local newVersion = baseVersion + 1
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

		if success and (wasAborted or result == nil) then
			return false
		end

		return success
	end

	local function FlushFilePath(filePath, releaseLock)
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

	local function FlushAllParallel()
		local pending = 0
		local startTime = os.clock()
		
		local filesToFlush = {}
		for f in pairs(dirtyFiles) do filesToFlush[f] = true end
		for f in pairs(activeLocks) do filesToFlush[f] = true end

		for filePath in pairs(filesToFlush) do
			pending += 1
			task.spawn(function()
				FlushFilePath(filePath, true) 
				pending -= 1
			end)
		end

		while pending > 0 and (os.clock() - startTime) < 25 do
			task.wait(0.1)
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
			local filesToRelease = {}
			
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

	local write = isRealFS and writefile or function(filePath, content)
		VirtualFS[filePath] = tostring(content or "")
		dirtyFiles[filePath] = true
	end

	local append
	if isRealFS then
		if typeof(appendfile) == "function" then
			append = appendfile
		else
			append = function(filePath, content)
				local existing = ""
				if typeof(isfile) == "function" and isfile(filePath) then
					local ok, res = pcall(readfile, filePath)
					if ok then existing = res end
				end
				writefile(filePath, existing .. tostring(content or ""))
			end
		end
	else
		append = function(filePath, content)
			VirtualFS[filePath] = (VirtualFS[filePath] or "") .. tostring(content or "")
			dirtyFiles[filePath] = true
		end
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

	local function WriteChunkedInternal(filePath, content, chunkSize)
		chunkSize = (type(chunkSize) == "number" and chunkSize) or 50000
		write(filePath, "")
		local totalLen = #content
		for i = 1, totalLen, chunkSize do
			append(filePath, string.sub(content, i, math.min(i + chunkSize - 1, totalLen)))
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
				local fileExists = true
				if item.opts.HasFile then
					if isRealFS and typeof(isfile) == "function" then
						fileExists = isfile(item.opts.FilePath or "")
					else
						fileExists = (VirtualFS[item.opts.FilePath or ""] ~= nil)
					end
				end

				if fileExists then
					local results = { pcall(item.fn, table.unpack(item.args)) }
					local ok = results[1]

					if ok then
						if item.callback then 
							task.spawn(item.callback, true, table.unpack(results, 2)) 
						end
					else
						local err = results[2]
						if onError then 
							task.spawn(onError, err, item.fn) 
						else 
							warn("Execution error: " .. tostring(err)) 
						end

						if item.callback then 
							task.spawn(item.callback, false, err) 
						end
					end
				else
					local err = "File does not exist"
					if onError then task.spawn(onError, err, item.fn) else warn("Execution error: " .. err) end
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
		local item = { fn = fn, args = opts.Args or {}, callback = opts.Callback, opts = opts }
		if opts.Priority then table.insert(queue, head, item) else table.insert(queue, item) end
		if not processing then task.spawn(process) end
	end

	local function SaveTableChunked(filePath, dataTable, chunkSize, callback)
		if type(chunkSize) == "function" then callback = chunkSize; chunkSize = 50000 end
		Load(function()
			local jsonStr = HttpService:JSONEncode(dataTable)
			if #jsonStr > MAX_DATASTORE_SIZE then error("Exceeds 4MB limit", 2) end
			task.wait()
			WriteChunkedInternal(filePath, jsonStr, chunkSize)
			return true
		end, { Callback = callback, FilePath = filePath })
	end

	local function ReadJsonChunked(filePath, callback)
		Load(function()
			return HttpService:JSONDecode(read(filePath))
		end, { Callback = callback, FilePath = filePath })
	end

	local function SavePlayerDataChunked(playerOrUserId, dataKey, dataTable, callback)
		local userId = typeof(playerOrUserId) == "Instance" and playerOrUserId.UserId or playerOrUserId
		SaveTableChunked(string.format("Player_%s_%s", tostring(userId), dataKey), dataTable, 50000, callback)
	end

	local function ReadPlayerDataChunked(playerOrUserId, dataKey, callback)
		local userId = typeof(playerOrUserId) == "Instance" and playerOrUserId.UserId or playerOrUserId
		ReadJsonChunked(string.format("Player_%s_%s", tostring(userId), dataKey), callback)
	end

	local self = {}
	self.Load = Load
	self.SaveTableChunked = SaveTableChunked
	self.ReadJsonChunked = ReadJsonChunked
	self.SavePlayerDataChunked = SavePlayerDataChunked
	self.ReadPlayerDataChunked = ReadPlayerDataChunked
	self.SetAutoSaveInterval = function(s) if type(s) == "number" and s > 0 then autoSaveInterval = s end end
	self.Flush = FlushAllParallel
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
