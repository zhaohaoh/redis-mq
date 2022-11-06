package com.redismq.autoconfigure;

import com.redismq.Message;
import com.redismq.connection.RedisClient;
import com.redismq.constant.RedisMQConstant;
import com.redismq.interceptor.ConsumeInterceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.RedisTemplate;

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
        if (size != null && size >= 100000) {
            log.error("RedisDeadQueueHandleInterceptor Full SIZE:{}", size);
        }
    }
}
