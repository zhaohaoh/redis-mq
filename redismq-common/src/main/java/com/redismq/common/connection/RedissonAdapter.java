package com.redismq.common.connection;

import com.redismq.common.pojo.Message;
import com.redismq.common.serializer.RedisMQStringMapper;
import org.redisson.api.RLock;
import org.redisson.api.RScoredSortedSet;
import org.redisson.api.RScript;
import org.redisson.api.RSet;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;
import org.redisson.client.protocol.ScoredEntry;
import org.springframework.util.CollectionUtils;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class RedissonAdapter implements RedisClient {

    private final RedissonClient redissonClient;

    public RedissonAdapter(RedissonClient redissonClient) {
        this.redissonClient = redissonClient;
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
        
        Object eval = redissonClient.getScript().eval(RScript.Mode.READ_WRITE, lua, RScript.ReturnType.INTEGER,
                keys.stream().map(a -> (Object) a).collect(Collectors.toList()), array);
        return Long.parseLong(eval.toString());
    }

    /**
     * 阻塞redis获取set集合中所有的元素
     */
    @Override
    public <T> Set<T> sMembers(String key, Class<T> tClass) {
        RSet<String> set = redissonClient.getSet(key, StringCodec.INSTANCE);
        Set<String> members = set.readAll();
        if (CollectionUtils.isEmpty(members)) {
            return new HashSet<>();
        }
        Set<T> zset = members.stream().map(a -> RedisMQStringMapper.toBean(a, tClass)).collect(Collectors.toSet());
        return zset;
    }

    /**
     * 转换并发送
     *
     * @param topic 主题
     * @param obj   obj
     */
    @Override
    public void convertAndSend(String topic, Object obj) {
        redissonClient.getTopic(topic).publish(RedisMQStringMapper.toJsonStr(obj));
    }

    /**
     * 删除key
     *
     * @param key
     */
    @Override
    public Boolean delete(String key) {
        redissonClient.getBucket(key).delete();
        return true;
    }


    /**
     * 批量删除key
     */
    @Override
    public Long delete(Collection<String> keys) {
        long delete = redissonClient.getKeys().delete(keys.toArray(new String[0]));
        return delete;
    }


    @Override
    public Boolean setIfAbsent(String key, Object value, Duration duration) {
        return redissonClient.getBucket(key).setIfAbsent(RedisMQStringMapper.toJsonStr(value), duration);
    }
    
    @Override
    public Object get(String key) {
        return redissonClient.getBucket(key).get();
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
        List<String> list = Arrays.stream(values).map(RedisMQStringMapper::toJsonStr).collect(Collectors.toList());
        boolean b = redissonClient.getSet(key).addAll(list);
        return 1L;
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
        List<String> list = Arrays.stream(values).map(RedisMQStringMapper::toJsonStr).collect(Collectors.toList());
        redissonClient.getSet(key).removeAll(list);
        return 1L;
    }

    @Override
    public Long mapCacheRemove(String key, String hashKey) {
        return redissonClient.getMapCache(key).fastRemove(hashKey);
    }
    
//    @Override
//    public boolean mapCachePut(String key, String hashKey, Object val,Duration duration) {
//        long millis = duration.toMillis();
//        redissonClient.getMapCache(key).put(hashKey, val, millis, TimeUnit.MILLISECONDS);
//        return true;
//    }
//
//    @Override
//    public Map<Object, Object> mapCacheList(String key, String hashKey) {
//        RList<Object> objects = redissonClient.getListMultimapCache(key).get(hashKey);
//        return objectObjectMap;
//    }
//
//    @Override
//    public List<Object> mapCacheList(String key) {
//        List<Object> all = redissonClient.getListMultimapCache(key).getAll(key);
//        return all;
//    }
    
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
        return redissonClient.getScoredSortedSet(key).add(score, RedisMQStringMapper.toJsonStr(value));
    }
    
    /**
     * 添加元素,有序集合是按照元素的score值由小到大排列
     *
     * @param key
     * @param value
     * @param score
     * @return
     */
    @Override
    public Boolean zAddIfAbsent(String key, Object value, double score) {
        return redissonClient.getScoredSortedSet(key).addIfAbsent(score, RedisMQStringMapper.toJsonStr(value));
    }

    /**
     * @param key
     * @param values
     * @return
     */
    @Override
    public Long zRemove(String key, Object... values) {
        List<String> collect = Arrays.stream(values).map(RedisMQStringMapper::toJsonStr).collect(Collectors.toList());
        redissonClient.getScoredSortedSet(key).removeAll(collect);
        return 1L;
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
        RScoredSortedSet<String> scoredSortedSet = redissonClient.getScoredSortedSet(key);
        Collection<ScoredEntry<String>> scoredEntries = scoredSortedSet.entryRange((int) start, (int) end);
        if (CollectionUtils.isEmpty(scoredEntries)) {
            return new HashMap<>();
        }
        Map<T, Double> newMap = new HashMap<>();
        for (ScoredEntry<String> typedTuple : scoredEntries) {
            T t = RedisMQStringMapper.toBean(typedTuple.getValue(), tClass);
            newMap.put(t, typedTuple.getScore());
        }
        return newMap;
    }
    
    /**
     * 获取zset的成员的分数
     *
     * @param key
     * @param start
     * @param end
     * @return
     */
    @Override
    public   Double zScore(String key,String member) {
        RScoredSortedSet<String> scoredSortedSet = redissonClient.getScoredSortedSet(key);
        Double score = scoredSortedSet.getScore(member);
        return score;
    }

    /**
     * 获取集合大小
     *
     * @param key
     * @return
     */
    @Override
    public Long zSize(String key) {
        int size = redissonClient.getScoredSortedSet(key).size();
        return (long) size;
    }


    /**
     * 根据指定的score值的范围来移除成员
     */
    @Override
    public Long zRemoveRangeByScore(String key, double min, double max) {
        int size = redissonClient.getScoredSortedSet(key).removeRangeByScore(min,true ,max,true);
        return (long) size;
    }
    
    @Override
    public Map<Message, Double> zrangeMessage(String key,String group,double min, double max, long start, long end) {
        String lua =
                "local data = redis.call('zrangebyscore', KEYS[2],ARGV[1], ARGV[2],'WITHSCORES', 'LIMIT', ARGV[3], ARGV[4]);\n"
                        + "\n" + "local result = {}\n" + "for i=1, #data, 2 do\n"
                        + "    local message = redis.call('hget', KEYS[1] .. ':body',data[i]);\n"
                        + "    if (message) then\n" + "        table.insert(result,message);\n"
                        + "        table.insert(result,data[i+1]);\n" + "    else\n"
                        + "        redis.call('zrem', KEYS[2],  data[i]);\n" + "    end\n" + "end\n" + "return result;";
        List<String> keys= new ArrayList<>();
        keys.add(key);
        keys.add(key+":"+group);
        
        Object[] array = new Object[4];
        array[0] = min;
        array[1] = max == 0D ? Double.MAX_VALUE : max;
        array[2] = start;
        array[3] = end == 0L ? Long.MAX_VALUE : end;
        List list = luaList(lua, keys, array);
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
        List eval = redissonClient.getScript().eval(RScript.Mode.READ_WRITE, lua, RScript.ReturnType.MULTI,
                keys.stream().map(a -> (Object) a).collect(Collectors.toList()), array);
        return eval;
    }
    
    @Override
    public Boolean exists(String key) {
        long countExists = redissonClient.getKeys().countExists(key);
        return countExists>0L;
    }
    //redisson是可重入锁。这边获取锁是同一个线程。不能用可重入锁
    @Override
    public Boolean lock(String key, String s, Duration duration) {
        RLock lock = redissonClient.getLock(key);
        //实现不可重入锁
        boolean heldByCurrentThread = lock.isHeldByCurrentThread();
        if (lock.isLocked() && heldByCurrentThread){
            return false;
        }
        long millis = duration.toMillis();
        try {
            return  lock.tryLock(0,millis, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
           Thread.currentThread().interrupt();
           return false;
        }
    }
    
    @Override
    public Boolean unlock(String key) {
        RLock lock = redissonClient.getLock(key);
        boolean b = lock.forceUnlock();
        return b;
    }
    
    @Override
    public Boolean isLock(String key) {
        RLock lock = redissonClient.getLock(key);
        return lock.isLocked();
    }
}
