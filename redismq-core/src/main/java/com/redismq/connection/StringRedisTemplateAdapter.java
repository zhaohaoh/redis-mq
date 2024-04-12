package com.redismq.connection;

import com.redismq.Message;
import com.redismq.utils.RedisMQStringMapper;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.util.CollectionUtils;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class StringRedisTemplateAdapter implements RedisClient {
    
    private final StringRedisTemplate stringRedisTemplate;
    
    public StringRedisTemplateAdapter(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }
    
    
    /**
     * 执行lua
     *
     * @param lua  lua
     * @param keys 键
     * @return {@link Long}
     */
    @Override
    public Long executeLua(String lua, List<String> keys, Object... args) {
        String[] array = Arrays.stream(args).filter(Objects::nonNull).map(RedisMQStringMapper::toJsonStr)
                .toArray(a -> new String[args.length]);
        
        DefaultRedisScript<Long> redisScript = new DefaultRedisScript<>(lua, Long.class);
        Long execute = stringRedisTemplate.execute(redisScript, keys, array);
        return execute;
    }
    
    /**
     * 阻塞redis获取set集合中所有的元素
     */
    @Override
    public <T> Set<T> sMembers(String key, Class<T> tClass) {
        Set<String> members = stringRedisTemplate.opsForSet().members(key);
        if (CollectionUtils.isEmpty(members)) {
            return new HashSet<>();
        }
        Set<T> set = members.stream().map(a -> RedisMQStringMapper.toBean(a, tClass)).collect(Collectors.toSet());
        return set;
    }
    
    /**
     * 转换并发送
     *
     * @param topic 主题
     * @param obj   obj
     */
    @Override
    public void convertAndSend(String topic, Object obj) {
        byte[] rawChannel = stringRedisTemplate.getStringSerializer().serialize(topic);
        RedisSerializer<Object> valueSerializer = (RedisSerializer<Object>) stringRedisTemplate.getValueSerializer();
        byte[] rawMessage = valueSerializer.serialize(RedisMQStringMapper.toJsonStr(obj));
    
        stringRedisTemplate.execute(connection -> {
            connection.publish(rawChannel, rawMessage);
            return null;
        }, true);
    }
    
    /**
     * 删除key
     *
     * @param key
     */
    @Override
    public Boolean delete(String key) {
        Boolean delete = stringRedisTemplate.delete(key);
        return delete;
    }
    
    
    /**
     * 批量删除key
     */
    @Override
    public Long delete(Collection<String> keys) {
        Long count = stringRedisTemplate.delete(keys);
        return count;
    }
    
    
    @Override
    public Boolean setIfAbsent(String key, Object value, Duration duration) {
  
        Boolean aBoolean = stringRedisTemplate.opsForValue()
                .setIfAbsent(key, RedisMQStringMapper.toJsonStr(value), duration);
        return aBoolean;
    }
    
    
    /**
     * set添加元素
     *
     * @param key
     * @param values
     * @return
     */
    @Override
    public Long sAdd(String key, Object... values) {
        String[] array = Arrays.stream(values).map(RedisMQStringMapper::toJsonStr)
                .toArray(a -> new String[values.length]);
        return stringRedisTemplate.opsForSet().add(key, array);
    }
    
    
    /**
     * set移除元素
     *
     * @param key
     * @param values
     * @return
     */
    @Override
    public Long sRemove(String key, Object... values) {
        String[] array = Arrays.stream(values).map(RedisMQStringMapper::toJsonStr)
                .toArray(a -> new String[values.length]);
        return stringRedisTemplate.opsForSet().remove(key, array);
    }
    
    /**------------------zSet相关操作--------------------------------*/
    
    /**
     * 添加元素,有序集合是按照元素的score值由小到大排列
     *
     * @param key
     * @param value
     * @param score
     * @return
     */
    @Override
    public Boolean zAdd(String key, Object value, double score) {
        return stringRedisTemplate.opsForZSet().add(key, RedisMQStringMapper.toJsonStr(value), score);
    }
    
    /**
     * @param key
     * @param values
     * @return
     */
    @Override
    public Long zRemove(String key, Object... values) {
        String[] array = Arrays.stream(values).map(RedisMQStringMapper::toJsonStr)
                .toArray(a -> new String[values.length]);
        return stringRedisTemplate.opsForZSet().remove(key, array);
    }
    
    
    @Override
    public Long hashRemove(String key, String hashKey) {
        return stringRedisTemplate.opsForHash().delete(key, hashKey);
    }
    
    
    /**
     * 获取集合元素, 并且把score值也获取
     *
     * @param key
     * @param start
     * @param end
     * @return
     */
    @Override
    public <T> Map<T, Double> zRangeWithScores(String key, long start, long end, Class<T> tClass) {
        Set<ZSetOperations.TypedTuple<String>> typedTuples = stringRedisTemplate.opsForZSet()
                .rangeWithScores(key, start, end);
        if (CollectionUtils.isEmpty(typedTuples)) {
            return new HashMap<>();
        }
        Map<T, Double> newMap = new HashMap<>();
        for (ZSetOperations.TypedTuple<String> typedTuple : typedTuples) {
            T t = RedisMQStringMapper.toBean(typedTuple.getValue(), tClass);
            newMap.put(t, typedTuple.getScore());
        }
        return newMap;
    }
    
    
    /**
     * 获取集合大小
     *
     * @param key
     * @return
     */
    @Override
    public Long zSize(String key) {
        return stringRedisTemplate.opsForZSet().size(key);
    }
    
    
    /**
     * 根据指定的score值的范围来移除成员
     *
     * @param key
     * @param min
     * @param max
     * @return
     */
    @Override
    public Long zRemoveRangeByScore(String key, double min, double max) {
        return stringRedisTemplate.opsForZSet().removeRangeByScore(key, min, max);
    }
    
    /**
     * 范围查找消息
     */
    @Override
    public Map<Message, Double> zrangeMessage(String key, double min, double max, long start, long end) {
        String lua =
                "local data = redis.call('zrangebyscore', KEYS[1],ARGV[1], ARGV[2],'WITHSCORES', 'LIMIT', ARGV[3], ARGV[4]);\n"
                        + "\n" + "local result = {}\n" + "for i=1, #data, 2 do\n"
                        + "    local message = redis.call('hget', KEYS[1] .. ':body',data[i]);\n"
                        + "    if (message) then\n" + "        table.insert(result,message);\n"
                        + "        table.insert(result,data[i+1]);\n" + "    else\n"
                        + "        redis.call('zrem', KEYS[1],  data[i]);\n" + "    end\n" + "end\n" + "return result;";
        Object[] array = new Object[4];
        array[0] = min;
        array[1] = max == 0D ? Double.MAX_VALUE : max;
        array[2] = start;
        array[3] = end == 0L ? Long.MAX_VALUE : end;
        List list = luaList(lua, Collections.singletonList(key), array);
        Map<Message, Double> newMap = new LinkedHashMap<>();
        for (int i = 0; i < list.size(); i += 2) {
            Object msgObj = list.get(i);
            Message message = RedisMQStringMapper.toBean(msgObj.toString(), Message.class);
            Object scope = list.get(i + 1);
            newMap.put(message, Double.valueOf(scope.toString()));
        }
        return newMap;
    }
    
    @Override
    public List luaList(String lua, List<String> keys, Object[] args) {
        String[] array = Arrays.stream(args).filter(Objects::nonNull).map(RedisMQStringMapper::toJsonStr)
                .toArray(a -> new String[args.length]);
        
        DefaultRedisScript<List> redisScript = new DefaultRedisScript<>(lua, List.class);
        List execute = stringRedisTemplate.execute(redisScript, keys, array);
        return execute;
    }
    
}
