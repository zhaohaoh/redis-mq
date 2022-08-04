package com.redismq.core;

import com.redismq.constant.PushMessage;
import com.redismq.utils.RedisMQObjectMapper;
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
        String msg = message.toString();
        String clientId = RedisMQObjectMapper.toBean(msg, String.class);
        if (!clientId.equals(redisMqClient.getClientId())) {
            redisMqClient.doRebalance();
        }
    }
 

}
