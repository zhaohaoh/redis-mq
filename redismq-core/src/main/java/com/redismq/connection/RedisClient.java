package com.redismq.connection;

import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.ZSetOperations;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * @Author: hzh
 * @Date: 2022/11/4 15:33
 * 连接redis客户端操作链
 */
public interface RedisClient {

    /**
     * 执行lua
     *
     * @param lua  lua
     * @param keys 键
     * @param args arg游戏
     * @return {@link Long}
     */
    Long executeLua(String lua,List<String> keys,Object... args);

    /**
     * 发布订阅
     *
     * @param topic 主题
     * @param obj   obj
     */
    void convertAndSend(String topic,Object obj);
    /**
     * 删除key
     *
     * @param key
     */
    Boolean delete(String key);


    /**
     * 批量删除key
     */
    Long delete(Collection<String> keys);


    /**
     * 是否存在key
     */
    Boolean exists(String key);

    /**
     * 设置过期时间
     */
    Boolean expire(String key, long timeout, TimeUnit unit);

    /**
     * 移除 key 的过期时间，key 将持久保持
     */
    Boolean persist(String key);

    /**
     * 返回 key 的剩余的过期时间
     */
    Long getExpire(String key, TimeUnit unit);

    /**
     * 返回 key 的剩余的过期时间
     */
    Long getExpire(String key);

    //  异步删除key
    Boolean unlink(String key);

    // 异步删除key
    Long unlink(Collection<String> key);

    /** -------------------string相关操作--------------------- */

    /**
     * 设置指定 key 的值
     */
    void set(String key, Object value);


    /**
     * 获取指定 key 的String值
     */
    String getStr(String key);

    /**
     * 获取指定 key 的值
     */
    @SuppressWarnings("unchecked")
    <T> T get(String key, Class<T> tClass);

    /**
     * 返回 key 中字符串值的子字符
     */
    String getRange(String key, long start, long end);

    /**
     * 将给定 key 的值设为 value ，并返回 key 的旧值(old value)
     */
    Object getAndSet(String key, Object value);

    /**
     * 批量获取
     */
    List<String> multiGetStr(Collection<String> keys);

    @SuppressWarnings("unchecked")
    <T> List<T> multiGet(Collection<String> keys, Class<T> tClass);

    List<Object> multiGetObj(Collection<String> keys);


    /**
     * 将值 value 关联到 key ，并将 key 的过期时间设为 timeout
     *
     * @param key
     * @param value
     * @param timeout 过期时间
     * @param unit    时间单位, 天:TimeUnit.DAYS 小时:TimeUnit.HOURS 分钟:TimeUnit.MINUTES
     *                秒:TimeUnit.SECONDS 毫秒:TimeUnit.MILLISECONDS
     */
    void setEx(String key, Object value, long timeout, TimeUnit unit);

    /**
     * 只有在 key 不存在时设置 key 的值
     *
     * @return 之前已经存在返回false, 不存在返回true
     */
    Boolean setIfAbsent(String key, Object value);
    /**
     * 只有在 key 不存在时设置 key 的值
     *
     * @return 之前已经存在返回false, 不存在返回true
     */
    Boolean setIfAbsent(String key, Object value, Duration duration);


    /**
     * 用 value 参数覆写给定 key 所储存的字符串值，从偏移量 offset 开始
     *
     * @param offset 从指定位置开始覆写
     */
    void setRange(String key, String value, long offset);

    /**
     * 获取字符串的长度
     *
     * @param key
     * @return
     */
    Long size(String key);

    /**
     * 批量添加
     *
     * @param maps
     */
    void multiSet(Map<String, Object> maps);

    /**
     * 同时设置一个或多个 key-value 对，当且仅当所有给定 key 都不存在
     *
     * @param maps
     * @return 之前已经存在返回false, 不存在返回true
     */
    boolean multiSetIfAbsent(Map<String, Object> maps);

    /**
     * 增加(自增长), 负数则为自减
     *
     * @param key
     * @return
     */
    Long increment(String key, long increment);


    /** --------------------set相关操作-------------------------- */

    /**
     * set添加元素
     *
     * @param key
     * @param values
     * @return
     */
    Long sAdd(String key, Object... values);

    Long sAdd(String key, List<Object> values);

    /**
     * set移除元素
     *
     * @param key
     * @param values
     * @return
     */
    Long sRemove(String key, Object... values);

    /**
     * 移除并返回集合的一个随机元素
     *
     * @param key
     * @return
     */
    Object sPop(String key);

    /**
     * 将元素value从一个集合移到另一个集合
     *
     * @param key
     * @param value
     * @param destKey
     * @return
     */
    Boolean sMove(String key, String value, String destKey);

    /**
     * 获取集合的大小
     *
     * @param key
     * @return
     */
    Long sSize(String key);

    /**
     * 判断集合是否包含value
     *
     * @param key
     * @param value
     * @return
     */
    Boolean sIsMember(String key, Object value);


    /**
     * 获取两个集合的交集
     *
     * @param key
     * @param otherKey
     * @return
     */
    Set<Object> sIntersect(String key, String otherKey);

    /**
     * 获取key集合与多个集合的交集
     *
     * @param key
     * @param otherKeys
     * @return
     */
    Set<Object> sIntersect(String key, Collection<String> otherKeys);

    /**
     * key集合与otherKey集合的交集存储到destKey集合中
     *
     * @param key
     * @param otherKey
     * @param destKey
     * @return
     */
    Long sIntersectAndStore(String key, String otherKey, String destKey);

    /**
     * key集合与多个集合的交集存储到destKey集合中
     *
     * @param key
     * @param otherKeys
     * @param destKey
     * @return
     */
    Long sIntersectAndStore(String key, Collection<String> otherKeys,
                            String destKey);

    /**
     * 获取两个集合的并集
     *
     * @param key
     * @param otherKeys
     * @return
     */
    Set<Object> sUnion(String key, String otherKeys);

    /**
     * 获取key集合与多个集合的并集
     *
     * @param key
     * @param otherKeys
     * @return
     */
    Set<Object> sUnion(String key, Collection<String> otherKeys);

    /**
     * key集合与otherKey集合的并集存储到destKey中
     */
    Long sUnionAndStore(String key, String otherKey, String destKey);

    /**
     * key集合与多个集合的并集存储到destKey中
     *
     * @param key
     * @param otherKeys
     * @param destKey
     * @return
     */
    Long sUnionAndStore(String key, Collection<String> otherKeys,
                        String destKey);

    /**
     * 获取两个集合的差集
     *
     * @param key
     * @param otherKey
     * @return
     */
    Set<Object> sDifference(String key, String otherKey);

    /**
     * 获取key集合与多个集合的差集
     *
     * @param key
     * @param otherKeys
     * @return
     */
    Set<Object> sDifference(String key, Collection<String> otherKeys);

    /**
     * key集合与otherKey集合的差集存储到destKey中
     *
     * @param key
     * @param otherKey
     * @param destKey
     * @return
     */
    Long sDifference(String key, String otherKey, String destKey);

    /**
     * key集合与多个集合的差集存储到destKey中
     *
     * @param key
     * @param otherKeys
     * @param destKey
     * @return
     */
    Long sDifference(String key, Collection<String> otherKeys,
                     String destKey);

    /**
     * 阻塞redis获取set集合中所有的元素
     */
    Set<Object> sMembers(String key);

    /**
     * 随机获取集合中的一个元素
     *
     * @param key
     * @return
     */
    Object sRandomMember(String key);

    /**
     * 随机获取集合中count个元素
     *
     * @param key
     * @param count
     * @return
     */
    List<Object> sRandomMembers(String key, long count);

    /**
     * 随机获取集合中count个元素并且去除重复的
     *
     * @param key
     * @param count
     * @return
     */
    Set<Object> sDistinctRandomMembers(String key, long count);

    /**
     * 不阻塞redis获取set集合中匹配的元素
     */
    Cursor<Object> sScan(String key, ScanOptions options);

    /**------------------zSet相关操作--------------------------------*/

    /**
     * 添加元素,有序集合是按照元素的score值由小到大排列
     *
     * @param key
     * @param value
     * @param score
     * @return
     */
    Boolean zAdd(String key, Object value, double score);

    /**
     * @param key
     * @param values
     * @return
     */
    Long zAdd(String key, Set<ZSetOperations.TypedTuple<Object>> values);


    /**
     * @param key
     * @param values
     * @return
     */
    Long zRemove(String key, Object... values);

    /**
     * 增加元素的score值，并返回增加后的值
     *
     * @param key
     * @param value
     * @param delta
     * @return
     */
    Double zIncrementScore(String key, String value, double delta);

    /**
     * 返回元素在集合的排名,有序集合是按照元素的score值由小到大排列
     *
     * @param key
     * @param value
     * @return 0表示第一位
     */
    Long zRank(String key, Object value);

    /**
     * 返回元素在集合的排名,按元素的score值由大到小排列
     *
     * @param key
     * @param value
     * @return
     */
    Long zReverseRank(String key, Object value);

    /**
     * 获取集合的元素, 从小到大排序
     *
     * @param key
     * @param start 开始位置
     * @param end   结束位置, -1查询所有
     * @return
     */
    Set<Object> zRange(String key, long start, long end);

    /**
     * 获取集合元素, 并且把score值也获取
     *
     * @param key
     * @param start
     * @param end
     * @return
     */
    Set<ZSetOperations.TypedTuple<Object>> zRangeWithScores(String key, long start,
                                                            long end);

    /**
     * 根据Score值查询集合元素
     *
     * @param key
     * @param min 最小值
     * @param max 最大值
     * @return
     */
    Set<Object> zRangeByScore(String key, double min, double max);

    /**
     * 根据Score值查询集合元素, 从小到大排序
     *
     * @param key
     * @param min 最小值
     * @param max 最大值
     * @return
     */
    Set<ZSetOperations.TypedTuple<Object>> zRangeByScoreWithScores(String key,
                                                                   double min, double max);

    /**
     * @param key
     * @param min
     * @param max
     * @param start
     * @param end
     * @return
     */
    Set<ZSetOperations.TypedTuple<Object>> zRangeByScoreWithScores(String key,
                                                                   double min, double max, long start, long end);

    /**
     * 获取集合的元素, 从大到小排序
     *
     * @param key
     * @param start
     * @param end
     * @return
     */
    Set<Object> zReverseRange(String key, long start, long end);

    Set<String> zReverseRangeStr(String key, long start, long end);

    /**
     * 获取集合的元素, 从大到小排序, 并返回score值
     *
     * @param key
     * @param start
     * @param end
     * @return
     */
    Set<ZSetOperations.TypedTuple<Object>> zReverseRangeWithScores(String key,
                                                                   long start, long end);

    /**
     * 根据Score值查询集合元素, 从大到小排序
     */
    Set<Object> zReverseRangeByScore(String key, double min,
                                     double max);

    /**
     * 根据Score值查询集合元素, 从大到小排序
     */
    Set<ZSetOperations.TypedTuple<Object>> zReverseRangeByScoreWithScores(
            String key, double min, double max);

    /**
     *
     */
    Set<Object> zReverseRangeByScore(String key, double min,
                                     double max, long start, long end);

    /**
     * 根据score值获取集合元素数量
     */
    Long zCount(String key, double min, double max);

    /**
     * 获取集合大小
     *
     * @param key
     * @return
     */
    Long zSize(String key);

    /**
     * 获取集合大小
     *
     * @param key
     * @return
     */
    Long zZCard(String key);

    /**
     * 获取集合中value元素的score值
     *
     * @param key
     * @param value
     * @return
     */
    Double zScore(String key, Object value);

    /**
     * 移除指定索引位置的成员
     *
     * @param key
     * @param start
     * @param end
     * @return
     */
    Long zRemoveRange(String key, long start, long end);

    /**
     * 根据指定的score值的范围来移除成员
     *
     * @param key
     * @param min
     * @param max
     * @return
     */
    Long zRemoveRangeByScore(String key, double min, double max);


}
