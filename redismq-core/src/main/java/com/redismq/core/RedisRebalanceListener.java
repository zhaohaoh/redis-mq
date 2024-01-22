package com.redismq.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;

/**
 * @Author: hzh
 * @Date: 2022/5/7 14:16
 * 重新负载均衡监听器
 */
public class RedisRebalanceListener implements MessageListener {
    protected static final Logger log = LoggerFactory.getLogger(RedisRebalanceListener.class);

    public RedisRebalanceListener(RedisMqClient redisMqClient) {
        this.redisMqClient = redisMqClient;
    }

    private final RedisMqClient redisMqClient;

    @Override
    public void onMessage(Message message, byte[] pattern) {
        log.info("RedisRebalanceListener clientId:{}", redisMqClient.getClientId());
        String clientId = message.toString();
        if (!clientId.equals(redisMqClient.getClientId())) {
            redisMqClient.doRebalance();
        }
    }
}
