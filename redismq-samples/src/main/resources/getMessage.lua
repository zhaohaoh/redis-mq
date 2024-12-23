local data = redis.call('zrangebyscore', KEYS[2],ARGV[1], ARGV[2],'WITHSCORES', 'LIMIT', ARGV[3], ARGV[4]);

local result = {}
for i=1, #data, 2 do
    local message = redis.call('hget', KEYS[1] .. ':body',data[i]);
    if (message) then
        table.insert(result,message);
        table.insert(result,data[i+1]);
    else
        redis.call('zrem', KEYS[2],  data[i]);
    end
end
return result;


--
--for i, v in pairs (data) do
--    local d=  redis.call('hget', KEYS[1] .. ':body', v);
--    table.insert(result1,d);
--    table.insert(result2,v);
--end