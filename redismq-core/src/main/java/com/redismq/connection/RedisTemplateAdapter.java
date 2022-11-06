package com.redismq.connection;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.*;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.util.CollectionUtils;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class RedisTemplateAdapter implements RedisClient {
    private   RedisTemplate<String, Object> redisTemplate;

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

    /**
     * 存在
     *
     * @param key 关键
     * @return {@link Boolean}
     */
    @Override
    public Boolean exists(String key) {
        return redisTemplate.hasKey(key);
    }


    /**
     * 设置过期时间
     */
    @Override
    public Boolean expire(String key, long timeout, TimeUnit unit) {
        return redisTemplate.expire(key, timeout, unit);
    }


    /**
     * 移除 key 的过期时间，key 将持久保持
     */
    @Override
    public Boolean persist(String key) {
        return redisTemplate.persist(key);
    }

    /**
     * 返回 key 的剩余的过期时间
     */
    @Override
    public Long getExpire(String key, TimeUnit unit) {
        return redisTemplate.getExpire(key, unit);
    }

    /**
     * 返回 key 的剩余的过期时间
     */
    @Override
    public Long getExpire(String key) {
        return redisTemplate.getExpire(key);
    }


    //  异步删除key
    @Override
    public Boolean unlink(String key) {
        return redisTemplate.unlink(key);
    }

    // 异步删除key
    @Override
    public Long unlink(Collection<String> key) {
        return redisTemplate.unlink(key);
    }

    /** -------------------string相关操作--------------------- */

    /**
     * 设置指定 key 的值
     */
    @Override
    public void set(String key, Object value) {
        redisTemplate.opsForValue().set(key, value);
    }


    /**
     * 获取指定 key 的String值
     */
    @Override
    public String getStr(String key) {
        Object value = redisTemplate.opsForValue().get(key);
        return value != null ? String.valueOf(value) : null;
    }

    /**
     * 获取指定 key 的值
     */
    @SuppressWarnings("unchecked")
    @Override
    public <T> T get(String key, Class<T> tClass) {
        return (T) redisTemplate.opsForValue().get(key);
    }

    /**
     * 返回 key 中字符串值的子字符
     */
    @Override
    public String getRange(String key, long start, long end) {
        return redisTemplate.opsForValue().get(key, start, end);
    }

    /**
     * 将给定 key 的值设为 value ，并返回 key 的旧值(old value)
     */
    @Override
    public Object getAndSet(String key, Object value) {
        return redisTemplate.opsForValue().getAndSet(key, value);
    }


    /**
     * 批量获取
     */
    @Override
    public List<String> multiGetStr(Collection<String> keys) {
        List<String> list = null;
        List<Object> objectList = redisTemplate.opsForValue().multiGet(keys);
        if (objectList != null) {
            list = objectList.stream()
                    .map(String::valueOf)
                    .collect(Collectors.toList());
        }
        return list;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> List<T> multiGet(Collection<String> keys, Class<T> tClass) {
        List<T> list = null;
        List<Object> objectList = redisTemplate.opsForValue().multiGet(keys);
        if (!CollectionUtils.isEmpty(objectList)) {
            list = objectList.stream().filter(Objects::nonNull)
                    .map(object -> (T) object)
                    .collect(Collectors.toList());
        }
        return list;
    }

    @Override
    public List<Object> multiGetObj(Collection<String> keys) {
        List<Object> objectList = redisTemplate.opsForValue().multiGet(keys);
        return objectList;
    }


    /**
     * 将值 value 关联到 key ，并将 key 的过期时间设为 timeout
     *
     * @param key
     * @param value
     * @param timeout 过期时间
     * @param unit    时间单位, 天:TimeUnit.DAYS 小时:TimeUnit.HOURS 分钟:TimeUnit.MINUTES
     *                秒:TimeUnit.SECONDS 毫秒:TimeUnit.MILLISECONDS
     */
    @Override
    public void setEx(String key, Object value, long timeout, TimeUnit unit) {
        //判断一个类是否是java的基本类型，Long啊什么的都给他转成字符串
        redisTemplate.opsForValue().set(key, value, timeout, unit);
    }

    /**
     * 只有在 key 不存在时设置 key 的值
     *
     * @return 之前已经存在返回false, 不存在返回true
     */
    @Override
    public Boolean setIfAbsent(String key, Object value) {
        return redisTemplate.opsForValue().setIfAbsent(key, value);
    }

    @Override
    public Boolean setIfAbsent(String key, Object value, Duration duration) {
        return redisTemplate.opsForValue().setIfAbsent(key,value,duration);
    }

    /**
     * 用 value 参数覆写给定 key 所储存的字符串值，从偏移量 offset 开始
     *
     * @param offset 从指定位置开始覆写
     */
    @Override
    public void setRange(String key, String value, long offset) {
        redisTemplate.opsForValue().set(key, value, offset);
    }

    /**
     * 获取字符串的长度
     *
     * @param key
     * @return
     */
    @Override
    public Long size(String key) {
        return redisTemplate.opsForValue().size(key);
    }

    /**
     * 批量添加
     *
     * @param maps
     */
    @Override
    public void multiSet(Map<String, Object> maps) {
        redisTemplate.opsForValue().multiSet(maps);
    }

    /**
     * 同时设置一个或多个 key-value 对，当且仅当所有给定 key 都不存在
     *
     * @param maps
     * @return 之前已经存在返回false, 不存在返回true
     */
    @Override
    public boolean multiSetIfAbsent(Map<String, Object> maps) {
        return redisTemplate.opsForValue().multiSetIfAbsent(maps);
    }

    /**
     * 增加(自增长), 负数则为自减
     *
     * @param key
     * @return
     */
    @Override
    public Long increment(String key, long increment) {
        return redisTemplate.opsForValue().increment(key, increment);
    }



    /** --------------------set相关操作-------------------------- */

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

    @Override
    public Long sAdd(String key, List<Object> values) {
        Object[] array = values.toArray(new Object[0]);
        return redisTemplate.opsForSet().add(key, array);
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

    /**
     * 移除并返回集合的一个随机元素
     *
     * @param key
     * @return
     */
    @Override
    public Object sPop(String key) {
        return redisTemplate.opsForSet().pop(key);
    }

    /**
     * 将元素value从一个集合移到另一个集合
     *
     * @param key
     * @param value
     * @param destKey
     * @return
     */
    @Override
    public Boolean sMove(String key, String value, String destKey) {
        return redisTemplate.opsForSet().move(key, value, destKey);
    }

    /**
     * 获取集合的大小
     *
     * @param key
     * @return
     */
    @Override
    public Long sSize(String key) {
        return redisTemplate.opsForSet().size(key);
    }

    /**
     * 判断集合是否包含value
     *
     * @param key
     * @param value
     * @return
     */
    @Override
    public Boolean sIsMember(String key, Object value) {
        return redisTemplate.opsForSet().isMember(key, value);
    }

    /**
     * 获取两个集合的交集
     *
     * @param key
     * @param otherKey
     * @return
     */
    @Override
    public Set<Object> sIntersect(String key, String otherKey) {
        return redisTemplate.opsForSet().intersect(key, otherKey);
    }

    /**
     * 获取key集合与多个集合的交集
     *
     * @param key
     * @param otherKeys
     * @return
     */
    @Override
    public Set<Object> sIntersect(String key, Collection<String> otherKeys) {
        return redisTemplate.opsForSet().intersect(key, otherKeys);
    }

    /**
     * key集合与otherKey集合的交集存储到destKey集合中
     *
     * @param key
     * @param otherKey
     * @param destKey
     * @return
     */
    @Override
    public Long sIntersectAndStore(String key, String otherKey, String destKey) {
        return redisTemplate.opsForSet().intersectAndStore(key, otherKey,
                destKey);
    }

    /**
     * key集合与多个集合的交集存储到destKey集合中
     *
     * @param key
     * @param otherKeys
     * @param destKey
     * @return
     */
    @Override
    public Long sIntersectAndStore(String key, Collection<String> otherKeys,
                                   String destKey) {
        return redisTemplate.opsForSet().intersectAndStore(key, otherKeys,
                destKey);
    }

    /**
     * 获取两个集合的并集
     *
     * @param key
     * @param otherKeys
     * @return
     */
    @Override
    public Set<Object> sUnion(String key, String otherKeys) {
        return redisTemplate.opsForSet().union(key, otherKeys);
    }

    /**
     * 获取key集合与多个集合的并集
     *
     * @param key
     * @param otherKeys
     * @return
     */
    @Override
    public Set<Object> sUnion(String key, Collection<String> otherKeys) {
        return redisTemplate.opsForSet().union(key, otherKeys);
    }

    /**
     * key集合与otherKey集合的并集存储到destKey中
     */
    @Override
    public Long sUnionAndStore(String key, String otherKey, String destKey) {
        return redisTemplate.opsForSet().unionAndStore(key, otherKey, destKey);
    }

    /**
     * key集合与多个集合的并集存储到destKey中
     *
     * @param key
     * @param otherKeys
     * @param destKey
     * @return
     */
    @Override
    public Long sUnionAndStore(String key, Collection<String> otherKeys,
                               String destKey) {
        return redisTemplate.opsForSet().unionAndStore(key, otherKeys, destKey);
    }

    /**
     * 获取两个集合的差集
     *
     * @param key
     * @param otherKey
     * @return
     */
    @Override
    public Set<Object> sDifference(String key, String otherKey) {
        return redisTemplate.opsForSet().difference(key, otherKey);
    }

    /**
     * 获取key集合与多个集合的差集
     *
     * @param key
     * @param otherKeys
     * @return
     */
    @Override
    public Set<Object> sDifference(String key, Collection<String> otherKeys) {
        return redisTemplate.opsForSet().difference(key, otherKeys);
    }

    /**
     * key集合与otherKey集合的差集存储到destKey中
     *
     * @param key
     * @param otherKey
     * @param destKey
     * @return
     */
    @Override
    public Long sDifference(String key, String otherKey, String destKey) {
        return redisTemplate.opsForSet().differenceAndStore(key, otherKey,
                destKey);
    }

    /**
     * key集合与多个集合的差集存储到destKey中
     *
     * @param key
     * @param otherKeys
     * @param destKey
     * @return
     */
    @Override
    public Long sDifference(String key, Collection<String> otherKeys,
                            String destKey) {
        return redisTemplate.opsForSet().differenceAndStore(key, otherKeys,
                destKey);
    }

    /**
     * 阻塞redis获取set集合中所有的元素
     */
    @Override
    public Set<Object> sMembers(String key) {
        return redisTemplate.opsForSet().members(key);
    }

    /**
     * 随机获取集合中的一个元素
     *
     * @param key
     * @return
     */
    @Override
    public Object sRandomMember(String key) {
        return redisTemplate.opsForSet().randomMember(key);
    }

    /**
     * 随机获取集合中count个元素
     *
     * @param key
     * @param count
     * @return
     */
    @Override
    public List<Object> sRandomMembers(String key, long count) {
        return redisTemplate.opsForSet().randomMembers(key, count);
    }

    /**
     * 随机获取集合中count个元素并且去除重复的
     *
     * @param key
     * @param count
     * @return
     */
    @Override
    public Set<Object> sDistinctRandomMembers(String key, long count) {
        return redisTemplate.opsForSet().distinctRandomMembers(key, count);
    }

    /**
     * 不阻塞redis获取set集合中匹配的元素
     */
    @Override
    public Cursor<Object> sScan(String key, ScanOptions options) {
        return redisTemplate.opsForSet().scan(key, options);
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
    public Long zAdd(String key, Set<ZSetOperations.TypedTuple<Object>> values) {
        return redisTemplate.opsForZSet().add(key, values);
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
     * 增加元素的score值，并返回增加后的值
     *
     * @param key
     * @param value
     * @param delta
     * @return
     */
    @Override

    public Double zIncrementScore(String key, String value, double delta) {
        return redisTemplate.opsForZSet().incrementScore(key, value, delta);
    }

    /**
     * 返回元素在集合的排名,有序集合是按照元素的score值由小到大排列
     *
     * @param key
     * @param value
     * @return 0表示第一位
     */
    @Override
    public Long zRank(String key, Object value) {
        return redisTemplate.opsForZSet().rank(key, value);
    }

    /**
     * 返回元素在集合的排名,按元素的score值由大到小排列
     *
     * @param key
     * @param value
     * @return
     */
    @Override
    public Long zReverseRank(String key, Object value) {
        return redisTemplate.opsForZSet().reverseRank(key, value);
    }

    /**
     * 获取集合的元素, 从小到大排序
     *
     * @param key
     * @param start 开始位置
     * @param end   结束位置, -1查询所有
     * @return
     */
    @Override
    public Set<Object> zRange(String key, long start, long end) {
        return redisTemplate.opsForZSet().range(key, start, end);
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
    public Set<ZSetOperations.TypedTuple<Object>> zRangeWithScores(String key, long start,
                                                                   long end) {
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

    /**
     * 根据Score值查询集合元素, 从小到大排序
     *
     * @param key
     * @param min 最小值
     * @param max 最大值
     * @return
     */
    @Override
    public Set<ZSetOperations.TypedTuple<Object>> zRangeByScoreWithScores(String key,
                                                                          double min, double max) {
        return redisTemplate.opsForZSet().rangeByScoreWithScores(key, min, max);
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
    public Set<ZSetOperations.TypedTuple<Object>> zRangeByScoreWithScores(String key,
                                                                          double min, double max, long start, long end) {
        return redisTemplate.opsForZSet().rangeByScoreWithScores(key, min, max,
                start, end);
    }

    /**
     * 获取集合的元素, 从大到小排序
     *
     * @param key
     * @param start
     * @param end
     * @return
     */
    @Override
    public Set<Object> zReverseRange(String key, long start, long end) {
        return redisTemplate.opsForZSet().reverseRange(key, start, end);
    }

    @Override

    public Set<String> zReverseRangeStr(String key, long start, long end) {
        Set<Object> set = redisTemplate.opsForZSet().reverseRange(key, start, end);
        if (CollectionUtils.isEmpty(set)) {
            return null;
        }
        return set.stream().map(Object::toString).collect(Collectors.toSet());
    }

    /**
     * 获取集合的元素, 从大到小排序, 并返回score值
     *
     * @param key
     * @param start
     * @param end
     * @return
     */
    @Override
    public Set<ZSetOperations.TypedTuple<Object>> zReverseRangeWithScores(String key,
                                                                          long start, long end) {
        return redisTemplate.opsForZSet().reverseRangeWithScores(key, start,
                end);
    }

    /**
     * 根据Score值查询集合元素, 从大到小排序
     */
    @Override
    public Set<Object> zReverseRangeByScore(String key, double min,
                                            double max) {
        return redisTemplate.opsForZSet().reverseRangeByScore(key, min, max);
    }

    /**
     * 根据Score值查询集合元素, 从大到小排序
     */
    @Override
    public Set<ZSetOperations.TypedTuple<Object>> zReverseRangeByScoreWithScores(
            String key, double min, double max) {
        return redisTemplate.opsForZSet().reverseRangeByScoreWithScores(key,
                min, max);
    }

    /**
     *
     */
    @Override
    public Set<Object> zReverseRangeByScore(String key, double min,
                                            double max, long start, long end) {
        return redisTemplate.opsForZSet().reverseRangeByScore(key, min, max,
                start, end);
    }

    /**
     * 根据score值获取集合元素数量
     */
    @Override
    public Long zCount(String key, double min, double max) {
        return redisTemplate.opsForZSet().count(key, min, max);
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
     * 获取集合大小
     *
     * @param key
     * @return
     */
    @Override
    public Long zZCard(String key) {
        return redisTemplate.opsForZSet().zCard(key);
    }

    /**
     * 获取集合中value元素的score值
     *
     * @param key
     * @param value
     * @return
     */
    @Override
    public Double zScore(String key, Object value) {
        return redisTemplate.opsForZSet().score(key, value);
    }

    /**
     * 移除指定索引位置的成员
     *
     * @param key
     * @param start
     * @param end
     * @return
     */
    @Override
    public Long zRemoveRange(String key, long start, long end) {
        return redisTemplate.opsForZSet().removeRange(key, start, end);
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
