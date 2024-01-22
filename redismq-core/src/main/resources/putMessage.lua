


-- 无备注版本
local messageZset = KEYS[1];
local messageBodyHashKey = KEYS[2];
local queueSize = ARGV[1];
local size = redis.call('zcard', messageZset);
if size and tonumber(size) >= tonumber(queueSize) then
return -1;
end
for i=2, #ARGV, 4 do
    redis.call('zadd', messageZset, ARGV[i],ARGV[i+1]);
    redis.call("hset", messageBodyHashKey, ARGV[i+2],ARGV[i+3] )
end
return size;