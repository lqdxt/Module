--[[
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
	local VirtualFS = {}
	local dirtyFiles = {}
	local StudioStore = nil
	local isRealFS = (typeof(writefile) == "function" and typeof(readfile) == "function")

	if not isRealFS and RunService:IsServer() then
		pcall(function()
			StudioStore = DataStoreService:GetDataStore("PlayerDataStore_v1")
		end)
	end

	local function requestWithRetry(actionFn, maxRetries)
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

	local function SaveToStudioStore(filePath, content)
		if not StudioStore or not content then return false end
		
		if #content > MAX_DATASTORE_SIZE then
			warn(string.format("[Queue] Cannot save '%s': Data size (%d bytes) exceeds 4MB limit!", filePath, #content))
			return false
		end
  
		local success = requestWithRetry(function()
			return StudioStore:UpdateAsync(filePath, function(_)
				return content
			end)
		end)
  
		return success
	end

	local function FlushFilePath(filePath)
		if not StudioStore or not dirtyFiles[filePath] then return end
		local content = VirtualFS[filePath]
		if content ~= nil then
			SaveToStudioStore(filePath, content)
		end
		dirtyFiles[filePath] = nil
	end

	local function FlushAllDirty()
		for filePath in pairs(dirtyFiles) do
			FlushFilePath(filePath)
			task.wait(0.5)
		end
	end

	if not isRealFS and RunService:IsServer() then
		Players.PlayerRemoving:Connect(function(player)
			local playerPrefix = "Player_" .. tostring(player.UserId)
			for filePath in pairs(dirtyFiles) do
				if string.find(filePath, playerPrefix, 1, true) then
					FlushFilePath(filePath)
				end
			end
		end)

		game:BindToClose(function()
			FlushAllDirty()
		end)
	end

	local Writefile = isRealFS and writefile or function(filePath, content)
		VirtualFS[filePath] = tostring(content or "")
		dirtyFiles[filePath] = true
	end

	local Appendfile = (isRealFS and typeof(appendfile) == "function") and appendfile or function(filePath, content)
		VirtualFS[filePath] = (VirtualFS[filePath] or "") .. tostring(content or "")
		dirtyFiles[filePath] = true
	end

	local Readfile = isRealFS and readfile or function(filePath)
		if VirtualFS[filePath] ~= nil then
			return VirtualFS[filePath]
		end
  
		if StudioStore then
			local success, data = requestWithRetry(function()
				return StudioStore:GetAsync(filePath)
			end)
			if success and data ~= nil then
				VirtualFS[filePath] = data
				return data
			end
		end
  
		error("File not found: " .. tostring(filePath), 2)
	end

	local queue = {}
	local head = 1
	local processing = false
	local paused = false
	local queueDelay = 0
	local onError = nil

	local function WriteChunkedInternal(filePath, content, chunkSize)
		chunkSize = (type(chunkSize) == "number" and chunkSize) or 50000
  
		Writefile(filePath, "")
  
		local totalLen = #content
		for i = 1, totalLen, chunkSize do
			local chunk = string.sub(content, i, math.min(i + chunkSize - 1, totalLen))
			Appendfile(filePath, chunk)
			task.wait()
		end
	end

	local function process()
		processing = true
  
		while head <= #queue do
			while paused do
				task.wait(0.1)
			end
   
			local item = queue[head]
			queue[head] = nil
			head += 1
   
			if item then
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
						warn("[Queue] Execution error: " .. tostring(err))
					end
     
					if item.callback then
						task.spawn(item.callback, false, err)
					end
				end
			end
   
			if head <= #queue and queueDelay > 0 then
				task.wait(queueDelay)
			end
		end
  
		table.clear(queue)
		head = 1
		processing = false
	end

	local function Load(fn, opts)
		if type(opts) == "boolean" then
			opts = { HasFile = opts }
		else
			opts = opts or {}
		end
  
		local item = {
			fn = fn,
			args = opts.Args or {},
			callback = opts.Callback,
		}
  
		if opts.Priority then
			table.insert(queue, head, item)
		else
			table.insert(queue, item)
		end
  
		if not processing then
			task.spawn(process)
		end
	end

	local function SaveTableChunked(filePath, dataTable, chunkSize, callback)
		if type(chunkSize) == "function" then
			callback = chunkSize
			chunkSize = 50000
		end
  
		Load(function()
			local jsonStr = HttpService:JSONEncode(dataTable)
			if #jsonStr > MAX_DATASTORE_SIZE then
				error("Data size exceeds 4MB DataStore limit", 2)
			end
			task.wait()
			WriteChunkedInternal(filePath, jsonStr, chunkSize)
			return true
		end, { Callback = callback })
	end

	local function ReadJsonChunked(filePath, callback)
		Load(function()
			local raw = Readfile(filePath)
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

	local self = {}
	self.Load = Load
	self.SaveTableChunked = SaveTableChunked
	self.ReadJsonChunked = ReadJsonChunked
	self.SavePlayerDataChunked = SavePlayerDataChunked
	self.ReadPlayerDataChunked = ReadPlayerDataChunked
	self.Flush = FlushAllDirty
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
