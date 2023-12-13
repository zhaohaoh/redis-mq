-- 通过hash和zset结合存储消息可以通过id删除。
local messageIdSet = KEYS[1];
local messageBodyMap = ARGV[2]
local publish = KEYS[2];
local publishValue = ARGV[1];
local zsetValue = ARGV[3];
local queueSize = ARGV[4];
-- 获取redis消息数量
local size = redis.call('zcard', messageIdSet);
if size and tonumber(size) >= queueSize then
return -1;
end
for k, v in pairs(zsetValue) do
    redis.call('zadd', messageIdSet, k,v);
end
-- 添加消息
for k, v in pairs(messageExtMap) do
    redis.call("hset", KEYS[4], k, v)
end
-- 发布订阅
redis.call('publish', publish, publishValue);
-- 返回消息的数量
return size;


local messageIdSet = KEYS[1];
local messageBodyMap = ARGV[2]
local publish = KEYS[2];
local publishValue = ARGV[1];
local zsetValue = ARGV[3];
local queueSize = ARGV[4];
local messageHash = KEYS[3];
local size = redis.call('zcard', messageIdSet);
if size and tonumber(size) >= queueSize then
return -1;
end
for k, v in pairs(zsetValue) do
    redis.call('zadd', messageIdSet, k,v);
end
for k, v in pairs(messageExtMap) do
    redis.call("hset", messageHash, k, v)
end
redis.call('publish', publish, publishValue);
return size;
