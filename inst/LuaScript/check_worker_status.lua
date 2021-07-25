local publicTaskQueue = KEYS[1]
local managerTaskSet = KEYS[2]
local workerIds = {}
for i = 1, #ARGV do
    workerIds[ARGV[i]] = ""
end

local missingTaskId = {}
local missingWorker = {}
local waitingTask = redis.call("SMEMBERS", managerTaskSet)
for i = 1, #waitingTask, 1 do
    local taskId = waitingTask[i]
    local workerId = redis.call("lrange", taskId, 2, 2)
    if (workerId ~= "") and (workerIds[workerId] == nil) then
        table.insert(missingTaskId, taskId)
        table.insert(missingWorker, workerId)
        redis.call('lset', taskId, 2, "")
        redis.call("rpush", publicTaskQueue, taskId)
    end
end

return missingWorker
