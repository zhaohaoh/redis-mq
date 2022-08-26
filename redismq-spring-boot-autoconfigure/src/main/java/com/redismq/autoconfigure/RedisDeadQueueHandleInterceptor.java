package com.redismq.autoconfigure;

import com.redismq.Message;
import com.redismq.constant.RedisMQConstant;
import com.redismq.interceptor.ConsumeInterceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.RedisTemplate;

public class RedisDeadQueueHandleInterceptor implements ConsumeInterceptor {
    private final RedisTemplate<String, Object> redisTemplate;
    protected static final Logger log = LoggerFactory.getLogger(RedisDeadQueueHandleInterceptor.class);

    public RedisDeadQueueHandleInterceptor(RedisTemplate<String, Object> redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    @Override
    public void onFail(Message message, Exception e) {
        String topic = message.getTopic();
        String deadQueue = RedisMQConstant.getDeadQueueNameByTopic(topic);
        redisTemplate.opsForZSet().add(deadQueue, message, System.currentTimeMillis());
        Long size = redisTemplate.opsForZSet().size(deadQueue);
        // 要加入告警自定义通知
        if (size != null && size >= 100000) {
            log.error("RedisDeadQueueHandleInterceptor Full SIZE:{}", size);
        }
    }
}
