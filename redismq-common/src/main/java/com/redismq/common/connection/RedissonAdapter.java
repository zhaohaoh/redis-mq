//package com.redismq.connection;
//
//import org.redisson.api.RScoredSortedSet;
//import org.redisson.api.RScript;
//import org.redisson.api.RedissonClient;
//import org.redisson.client.protocol.ScoredEntry;
//import org.springframework.util.CollectionUtils;
//
//import java.time.Duration;
//import java.util.Arrays;
//import java.util.Collection;
//import java.util.HashMap;
//import java.util.HashSet;
//import java.util.LinkedHashMap;
//import java.util.List;
//import java.util.Map;
//import java.util.Set;
//import java.util.stream.Collectors;
//
//public class RedissonAdapter implements RedisClient {
//
//    private final RedissonClient redissonClient;
//
//    public RedissonAdapter(RedissonClient redissonClient) {
//        this.redissonClient = redissonClient;
//    }
//
//    /**
//     * 执行lua
//     *
//     * @param lua  lua
//     * @param keys 键
//     * @return {@link Long}
//     */
//    @Override
//    public Long executeLua(String lua, List<String> keys, Object... args) {
//        Object eval = redissonClient.getScript().eval(RScript.Mode.READ_WRITE, lua, RScript.ReturnType.INTEGER,
//                keys.stream().map(a -> (Object) a).collect(Collectors.toList()), args);
//        return Long.parseLong(eval.toString());
//    }
//
//    /**
//     * 阻塞redis获取set集合中所有的元素
//     */
//    @Override
//    public <T> Set<T> sMembers(String key, Class<T> tClass) {
//        Set<T> members = redissonClient.getSet(key);
//        if (CollectionUtils.isEmpty(members)) {
//            return new HashSet<>();
//        }
//        return members;
//    }
//
//    /**
//     * 转换并发送
//     *
//     * @param topic 主题
//     * @param obj   obj
//     */
//    @Override
//    public void convertAndSend(String topic, Object obj) {
//        redissonClient.getTopic(topic).publish(obj);
//    }
//
//    /**
//     * 删除key
//     *
//     * @param key
//     */
//    @Override
//    public Boolean delete(String key) {
//        redissonClient.getBucket(key).delete();
//        return true;
//    }
//
//
//    /**
//     * 批量删除key
//     */
//    @Override
//    public Long delete(Collection<String> keys) {
//        long delete = redissonClient.getKeys().delete(keys.toArray(new String[0]));
//        return delete;
//    }
//
//
//    @Override
//    public Boolean setIfAbsent(String key, Object value, Duration duration) {
//        return redissonClient.getBucket(key).setIfAbsent(value, duration);
//    }
//
//
//    /**
//     * set添加元素
//     *
//     * @param key
//     * @param values
//     * @return
//     */
//    @Override
//    public Long sAdd(String key, Object... values) {
//        boolean b = redissonClient.getSet(key).addAll(Arrays.stream(values).collect(Collectors.toList()));
//        return 1L;
//    }
//
//
//    /**
//     * set移除元素
//     *
//     * @param key
//     * @param values
//     * @return
//     */
//    @Override
//    public Long sRemove(String key, Object... values) {
//        redissonClient.getSet(key).removeAll(Arrays.stream(values).collect(Collectors.toList()));
//        return 1L;
//    }
//
//    @Override
//    public Long hashRemove(String key, String hashKey) {
//        return redissonClient.getMap(key).fastRemove(hashKey);
//    }
//
//
//    /**------------------zSet相关操作--------------------------------*/
//
//    /**
//     * 添加元素,有序集合是按照元素的score值由小到大排列
//     *
//     * @param key
//     * @param value
//     * @param score
//     * @return
//     */
//    @Override
//    public Boolean zAdd(String key, Object value, double score) {
//        return redissonClient.getScoredSortedSet(key).add(score, value);
//    }
//
//    /**
//     * @param key
//     * @param values
//     * @return
//     */
//    @Override
//    public Long zRemove(String key, Object... values) {
//        List<Object> collect = Arrays.stream(values).collect(Collectors.toList());
//        redissonClient.getScoredSortedSet(key).removeAll(collect);
//        return 1L;
//    }
//
//
//    /**
//     * 获取集合元素, 并且把score值也获取
//     *
//     * @param key
//     * @param start
//     * @param end
//     * @return
//     */
//    @Override
//    public <T> Map<T, Double> zRangeWithScores(String key, long start, long end, Class<T> tClass) {
//        RScoredSortedSet<T> scoredSortedSet = redissonClient.getScoredSortedSet(key);
//        Collection<ScoredEntry<T>> scoredEntries = scoredSortedSet.entryRange(start, true, end, true);
//        if (CollectionUtils.isEmpty(scoredEntries)) {
//            return new HashMap<>();
//        }
//        Map<T, Double> newMap = new HashMap<>();
//        for (ScoredEntry<T> typedTuple : scoredEntries) {
//            newMap.put(typedTuple.getValue(), typedTuple.getScore());
//        }
//        return newMap;
//    }
//
//    /**
//     * 根据Score值查询集合元素
//     *
//     * @param key
//     * @param min 最小值
//     * @param max 最大值
//     * @return
//     */
//    @Override
//    public <T> Set<T> zRangeByScore(String key, double min, double max, Class<T> tClass) {
//        RScoredSortedSet<T> scoredSortedSet = redissonClient.getScoredSortedSet(key);
//        Collection<ScoredEntry<T>> scoredEntries = scoredSortedSet.entryRange(min, true, max, true);
//        return scoredEntries.stream().map(ScoredEntry::getValue).collect(Collectors.toSet());
//    }
//
//    @Override
//    public <T> Set<T> zRangeByScore(String key, double min, double max, long start, long end, Class<T> tClass) {
//        RScoredSortedSet<T> scoredSortedSet = redissonClient.getScoredSortedSet(key);
//        Collection<ScoredEntry<T>> scoredEntries = scoredSortedSet.entryRange(min, true, max, true,(int)start,(int)end);
//        if (CollectionUtils.isEmpty(scoredEntries)) {
//            return new HashSet<>();
//        }
//        return scoredEntries.stream().map(ScoredEntry::getValue).collect(Collectors.toSet());
//    }
//
//
//    /**
//     * @param key
//     * @param min
//     * @param max
//     * @param start
//     * @param end
//     * @return
//     */
//    @Override
//    public <T> Map<T, Double> zRangeByScoreWithScores(String key, double min, double max, long start, long end,
//            Class<T> tClass) {
//        RScoredSortedSet<T> scoredSortedSet = redissonClient.getScoredSortedSet(key);
//        Collection<ScoredEntry<T>> scoredEntries = scoredSortedSet.entryRange(min, true, max, true,(int)start,(int)end);
//        if (CollectionUtils.isEmpty(scoredEntries)) {
//            return new HashMap<>();
//        }
//        Map<T, Double> newMap = new LinkedHashMap<>();
//        for (ScoredEntry<T> typedTuple : scoredEntries) {
//            newMap.put(typedTuple.getValue(), typedTuple.getScore());
//        }
//        return newMap;
//    }
//
//
//    /**
//     * 获取集合大小
//     *
//     * @param key
//     * @return
//     */
//    @Override
//    public Long zSize(String key) {
//        int size = redissonClient.getScoredSortedSet(key).size();
//        return (long) size;
//    }
//
//
//    /**
//     * 根据指定的score值的范围来移除成员
//     *
//     * @param key
//     * @param min
//     * @param max
//     * @return
//     */
//    @Override
//    public Long zRemoveRangeByScore(String key, double min, double max) {
//        int size = redissonClient.getScoredSortedSet(key).removeRangeByScore(min,true ,max,true);
//        return (long) size;
//    }
//}
