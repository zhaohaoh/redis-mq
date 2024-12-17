
-- 无备注版本   -- 先判断每个分组下的队列是否有阻塞
local messageZsets = KEYS[1];
local queueSize = ARGV[1];
local size = 0;
for messageZset in messageZsets:gmatch("([^,]+)") do
    size = redis.call('zcard', messageZset);
    if size and tonumber(size) >= tonumber(queueSize) then
        return -1;
    end
end
local messageBodyHashKey = KEYS[2];

for i=2, #ARGV, 4 do
    for messageZset in messageZsets:gmatch("([^,]+)") do
        redis.call('zadd', messageZset, ARGV[i],ARGV[i+1]);
    end
    redis.call("hset", messageBodyHashKey, ARGV[i+2],ARGV[i+3] )
end
return size;