package com.redismq.core;


import org.springframework.data.redis.connection.MessageListener;

import java.util.concurrent.Semaphore;

/**
 * @Author: hzh
 * @Date: 2022/4/28 12:16
 * 抽象push监听类
 */
public abstract class AbstractRedisPushListener implements MessageListener {
    public AbstractRedisPushListener(RedisMqClient redisMqClient) {
        this.redisMqClient = redisMqClient;
    }

    protected Semaphore semaphore = new Semaphore(5);
    protected RedisMqClient redisMqClient;

}
