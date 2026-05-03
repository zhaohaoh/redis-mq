package com.redismq.interceptor;

import com.redismq.common.connection.RedisMQClientUtil;
import com.redismq.common.pojo.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RedisDeadQueueHandleInterceptor implements ConsumeInterceptor {
    private final RedisMQClientUtil redisMQClientUtil;
    protected static final Logger log = LoggerFactory.getLogger(RedisDeadQueueHandleInterceptor.class);

    public RedisDeadQueueHandleInterceptor(RedisMQClientUtil redisMQClientUtil) {
        this.redisMQClientUtil = redisMQClientUtil;
    }

    @Override
    public void onFail(Message message, Exception e) {
        redisMQClientUtil.putDeadQueue(message);
        Long size = redisMQClientUtil.deadQueueSize(message.getVirtualQueueName());
        if (size != null && size >= 100000) {
            log.error("RedisDeadQueueHandleInterceptor is Full SIZE:{}  message:{}", size, message);
        }
    }
}
