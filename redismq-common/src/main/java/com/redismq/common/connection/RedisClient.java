package com.redismq.common.connection;

import com.redismq.common.pojo.Message;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author hzh
 * @date 2024/7/19 10:39
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
     * 阻塞redis获取set集合中所有的元素
     */
    <T> Set<T> sMembers(String key,Class<T> tClass) ;
    /**
     * 批量删除key
     */
    Long delete(Collection<String> keys);
    
    /**
     * 只有在 key 不存在时设置 key 的值
     *
     * @return 之前已经存在返回false, 不存在返回true
     */
    Boolean setIfAbsent(String key, Object value, Duration duration);
 
    /**
     * set添加元素
     *
     * @param key
     * @param values
     * @return
     */
    Long sAdd(String key, Object... values);
 
    /**
     * set移除元素
     *
     * @param key
     * @param values
     * @return
     */
    Long sRemove(String key, Object... values);
    
    Long hashRemove(String key, String hashKey);
    
    
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
    Long zRemove(String key, Object... values);

   

    /**
     * 获取集合元素, 并且把score值也获取
     *
     * @param key
     * @param start
     * @param end
     * @return
     */
    <T> Map<T,Double> zRangeWithScores(String key, long start,
                                                            long end,Class<T> tClass);
 
    
 
 

   
    /**
     * 获取集合大小
     *
     * @param key
     * @return
     */
    Long zSize(String key);

    

    /**
     * 根据指定的score值的范围来移除成员
     *
     * @param key
     * @param min
     * @param max
     * @return
     */
    Long zRemoveRangeByScore(String key, double min, double max);
    
    /**
     * 拉取消息
     *
     * @param key   钥匙
     * @param min   最小
     * @param max   最大值
     * @param start 开始
     * @param end   终止
     * @return {@link Map}<{@link Message}, {@link Double}>
     */
    Map<Message, Double> zrangeMessage(String key, double min, double max, long start, long end);
    
    
    /**
     * lua列表
     *
     * 返回list的lua
     */
    List luaList(String lua, List<String> keys, Object[] args);
    
    Boolean exists(String key);
}
