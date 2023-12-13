package com.redismq.connection;

import com.redismq.utils.RedisMQStringMapper;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.util.CollectionUtils;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
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
    public <T> Set<T> sMembers(String key,Class<T> tClass) {
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
        stringRedisTemplate.convertAndSend(topic, RedisMQStringMapper.toJsonStr(obj));
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
        return stringRedisTemplate.opsForValue().setIfAbsent(key, RedisMQStringMapper.toJsonStr(value), duration);
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
     * 根据Score值查询集合元素
     *
     * @param key
     * @param min 最小值
     * @param max 最大值
     * @return
     */
    @Override
    public <T> Set<T> zRangeByScore(String key, double min, double max, Class<T> tClass) {
        Set<String> sets = stringRedisTemplate.opsForZSet().rangeByScore(key, min, max);
        if (CollectionUtils.isEmpty(sets)) {
            return new HashSet<>();
        }
        return sets.stream().map(a -> RedisMQStringMapper.toBean(a, tClass)).collect(Collectors.toSet());
    }
    
    @Override
    public <T> Set<T> zRangeByScore(String key, double min, double max, long start, long end, Class<T> tClass) {
        Set<String> sets = stringRedisTemplate.opsForZSet().rangeByScore(key, min, max, start, end);
        if (CollectionUtils.isEmpty(sets)) {
            return new HashSet<>();
        }
        return sets.stream().map(a -> RedisMQStringMapper.toBean(a, tClass)).collect(Collectors.toSet());
    }
    
    
    /**
     * @param key
     * @param min
     * @param max
     * @param start
     * @param end
     * @return
     */
    @Override
    public <T> Map<T, Double> zRangeByScoreWithScores(String key, double min, double max, long start, long end,
            Class<T> tClass) {
        Set<ZSetOperations.TypedTuple<String>> typedTuples = stringRedisTemplate.opsForZSet()
                .rangeByScoreWithScores(key, min, max, start, end);
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
}
