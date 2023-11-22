package com.redismq.connection;

import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.data.redis.core.script.DefaultRedisScript;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Set;

public class RedisTemplateAdapter implements RedisClient {
    
    private RedisTemplate<String, Object> redisTemplate;
    
    public RedisTemplateAdapter(RedisTemplate<String, Object> redisTemplate) {
        this.redisTemplate = redisTemplate;
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
        DefaultRedisScript<Long> redisScript = new DefaultRedisScript<>(lua, Long.class);
        return redisTemplate.execute(redisScript, keys, args);
    }
    /**
     * 阻塞redis获取set集合中所有的元素
     */
    @Override
    public Set<Object> sMembers(String key) {
        return redisTemplate.opsForSet().members(key);
    }
    /**
     * 转换并发送
     *
     * @param topic 主题
     * @param obj   obj
     */
    @Override
    public void convertAndSend(String topic, Object obj) {
        redisTemplate.convertAndSend(topic, obj);
    }
    
    /**
     * 删除key
     *
     * @param key
     */
    @Override
    public Boolean delete(String key) {
        Boolean delete = redisTemplate.delete(key);
        return delete;
    }
    
    
    /**
     * 批量删除key
     */
    @Override
    public Long delete(Collection<String> keys) {
        Long count = redisTemplate.delete(keys);
        return count;
    }
 
    
    
    @Override
    public Boolean setIfAbsent(String key, Object value, Duration duration) {
        return redisTemplate.opsForValue().setIfAbsent(key, value, duration);
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
        return redisTemplate.opsForSet().add(key, values);
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
        return redisTemplate.opsForSet().remove(key, values);
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
        return redisTemplate.opsForZSet().add(key, value, score);
    }
    
    /**
     * @param key
     * @param values
     * @return
     */
    @Override
    public Long zRemove(String key, Object... values) {
        return redisTemplate.opsForZSet().remove(key, values);
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
    public Set<ZSetOperations.TypedTuple<Object>> zRangeWithScores(String key, long start, long end) {
        return redisTemplate.opsForZSet().rangeWithScores(key, start, end);
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
    public Set<Object> zRangeByScore(String key, double min, double max) {
        return redisTemplate.opsForZSet().rangeByScore(key, min, max);
    }
    
    @Override
    public Set<Object> zRangeByScore(String key, double min, double max, long start, long end) {
        return redisTemplate.opsForZSet().rangeByScore(key, min, max, start, end);
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
    public Set<ZSetOperations.TypedTuple<Object>> zRangeByScoreWithScores(String key, double min, double max,
            long start, long end) {
        return redisTemplate.opsForZSet().rangeByScoreWithScores(key, min, max, start, end);
    }
    
    
    /**
     * 获取集合大小
     *
     * @param key
     * @return
     */
    @Override
    public Long zSize(String key) {
        return redisTemplate.opsForZSet().size(key);
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
        return redisTemplate.opsForZSet().removeRangeByScore(key, min, max);
    }
}
