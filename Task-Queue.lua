--[[
IF YOU'RE IN STUDIO MAKE SURE TO REMOVE 'isRealFS' AND WHERE ITS USED
local writefile = isRealFS and writefile or function --> local writefile = function
same with appendfile, readfile
]]
local Queue = (function()
	local HttpService = game:GetService("HttpService")
	local RunService = game:GetService("RunService")
	local DataStoreService = game:GetService("DataStoreService")

	local VirtualFS = {}
	local StudioStore = nil
	local isRealFS = (typeof(writefile) == "function" and typeof(readfile) == "function")

	if not isRealFS and RunService:IsServer() then
		pcall(function()
			StudioStore = DataStoreService:GetDataStore("StudioVirtualFS")
		end)
	end

	local function SaveToStudioStore(filePath, content)
		if StudioStore then
			pcall(function()
				StudioStore:SetAsync(filePath, content)
			end)
		end
	end

	local writefile = isRealFS and writefile or function(filePath, content)
		VirtualFS[filePath] = tostring(content or "")
	end

	local appendfile = (isRealFS and typeof(appendfile) == "function") and appendfile or function(filePath, content)
		VirtualFS[filePath] = (VirtualFS[filePath] or "") .. tostring(content or "")
	end

	local readfile = isRealFS and readfile or function(filePath)
		if VirtualFS[filePath] ~= nil then
			return VirtualFS[filePath]
		end

		if StudioStore then
			local ok, data = pcall(function()
				return StudioStore:GetAsync(filePath)
			end)
			if ok and data ~= nil then
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

		writefile(filePath, "")

		local totalLen = #content
		for i = 1, totalLen, chunkSize do
			local chunk = string.sub(content, i, math.min(i + chunkSize - 1, totalLen))
			appendfile(filePath, chunk)
			task.wait()
		end

		if not isRealFS then
			SaveToStudioStore(filePath, VirtualFS[filePath])
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
						warn("[Queue] Function execution error: " .. tostring(err))
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

	local function WriteFileChunked(filePath, content, chunkSize, callback)
		if type(chunkSize) == "function" then
			callback = chunkSize
			chunkSize = 50000
		end

		Load(function()
			WriteChunkedInternal(filePath, content, chunkSize)
			return true
		end, { Callback = callback })
	end

	local function SaveTableChunked(filePath, dataTable, chunkSize, callback)
		if type(chunkSize) == "function" then
			callback = chunkSize
			chunkSize = 50000
		end

		Load(function()
			local jsonStr = HttpService:JSONEncode(dataTable)
			task.wait()
			WriteChunkedInternal(filePath, jsonStr, chunkSize)
			return true
		end, { Callback = callback })
	end

	local function ReadJsonChunked(filePath, callback)
		Load(function()
			local raw = readfile(filePath)
			task.wait()
			return HttpService:JSONDecode(raw)
		end, { Callback = callback })
	end

	local function Pause() paused = true end
	local function Resume() paused = false end
	local function Clear() table.clear(queue) head = 1 end
	local function SetDelay(seconds) queueDelay = seconds or 0 end
	local function SetErrorHandler(fn) onError = fn end
	local function GetQueueLength() return (#queue - head + 1) end
	local function IsProcessing() return processing end

	local self = {}
	self.Load = Load
	self.WriteFileChunked = WriteFileChunked
	self.SaveTableChunked = SaveTableChunked
	self.ReadJsonChunked = ReadJsonChunked
	self.Pause = Pause
	self.Resume = Resume
	self.Clear = Clear
	self.SetDelay = SetDelay
	self.SetErrorHandler = SetErrorHandler
	self.GetQueueLength = GetQueueLength
	self.IsProcessing = IsProcessing
	return self
end)()

return Queue
