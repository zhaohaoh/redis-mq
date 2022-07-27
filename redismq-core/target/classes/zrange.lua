local expiredValues = redis.call('zrangebyscore', KEYS[2], 0, ARGV[1], 'limit', 0, ARGV[2]);
if #expiredValues > 0 then
local count = redis.call('zrem', KEYS[2], unpack(expiredValues));