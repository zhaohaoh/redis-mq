-- 1.获取删除消息的偏移量
-- 2.删除组内消息id
-- 3.判断此次消息的偏移量是否大于当前消息组偏移量，进行替换
local result={};
local messageIdQueue=KEYS[1];
local queueGroups= KEYS[3];
local offsetGroup= KEYS[4];
local orginalQueueName =KEYS[5];
local otherMessageIdQueues =KEYS[6];
local diffMax =KEYS[7];
local msgOffset =KEYS[8];

local r1 = redis.call('zrem', messageIdQueue, ARGV[1]);
table.insert(result,r1);

local currentOffset = redis.call('ZSCORE', offsetGroup,orginalQueueName);
if currentOffset and msgOffset and  tonumber(msgOffset) > tonumber(currentOffset) then
    redis.call('zadd', offsetGroup, msgOffset,orginalQueueName);
end

local groupAck = true;
for queueGroup in queueGroups:gmatch("([^,]+)") do
    local res = redis.call('ZSCORE', queueGroup, ARGV[1]);
    if res then
        groupAck = false;
    end
end

if groupAck then
    local r2  = redis.call('hdel', KEYS[2],  ARGV[1]);
    table.insert(result,r2);
end
local size = redis.call('HLEN',KEYS[2]);
if size>0 then
    for msgQueue in otherMessageIdQueues:gmatch("([^,]+)") do
        local  data = redis.call('ZRANGEBYSCORE',msgQueue,0,currentOffset - (tonumber(diffMax)-1));
        for i, messageId in ipairs(data) do
            redis.call('zrem',msgQueue,messageId);
            local r2  = redis.call('hdel', KEYS[2],  messageId);
            table.insert(result,r2);
        end
    end
end
return result;
