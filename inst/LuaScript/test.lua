local publicTasks = redis.call("lrange", "RedisParam_public_task_queue_test", 1, -1)
return existsElement(publicTasks, "RedisParam_task_10ffc4f6-61a1-4c8d-965b-8e174849f969")
