package com.redismq.constant;

public interface RedisMQBeanNameConstant {
    /**
     * redismq操作模板类名
     */
    String REDISMQ_REDIS_TEMPLATE = "redisMQRedisTemplate";
    /**
     * redismq消息listenercontainer
     */
    String REDISMQ_MESSAGE_LISTENERCONTAINER = "redisMQMessageListenerContainer";
    /**
     * redismq内部消息listenercontainer
     */
    String REDISMQ_INNER_MESSAGE_LISTENERCONTAINER = "redismqInnerRedisMessageListenerContainer";
    /**
     * redismq的 连接工厂connectionfactory
     */
    String REDISMQ_CONNECTIONFACTORY = "RedisMQConnectionFactory";

    /**
     *  redismq的lettuce源数据
     */
    String REDISMQ_LETTUCE_CLIENTRESOURCES = "RedisMQLettuceClientResources";
}
