package com.redismq.interceptor;

import com.redismq.Message;
import com.redismq.connection.RedisClient;
import com.redismq.constant.RedisMQConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author: hzh
 * @Date: 2022/12/8 10:16
 * 提供默认的对于消费失败的死信队列实现
 */
public class RedisDeadQueueHandleInterceptor implements ConsumeInterceptor {
    private final RedisClient redisClient;
    protected static final Logger log = LoggerFactory.getLogger(RedisDeadQueueHandleInterceptor.class);

    public RedisDeadQueueHandleInterceptor(RedisClient redisClient) {
        this.redisClient = redisClient;
    }

    @Override
    public void onFail(Message message, Exception e) {
        String topic = message.getTopic();
        String deadQueue = RedisMQConstant.getDeadQueueNameByTopic(topic);
        redisClient.zAdd(deadQueue, message, System.currentTimeMillis());
        Long size = redisClient.zSize(deadQueue);
        // 要加入告警自定义通知
        if (size != null && size >= 1000000) {
            log.error("RedisDeadQueueHandleInterceptor is Full SIZE:{}  message:{}", size, message);
        }
    }
}
