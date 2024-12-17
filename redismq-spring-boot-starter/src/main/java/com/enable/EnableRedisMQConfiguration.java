package com.enable;

import com.redismq.autoconfigure.RedisMQProperties;
import com.redismq.common.connection.RedisMQClientUtil;
import com.redismq.core.RedisListenerContainerManager;
import com.redismq.core.RedisMQProducer;
import com.redismq.core.RedisMqClient;
import com.redismq.id.WorkIdGenerator;
import com.redismq.rebalance.AllocateMessageQueueAveragely;
import com.redismq.rebalance.QueueRebalanceImpl;
import com.redismq.rpc.client.RemotingClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;

import static com.redismq.common.constant.RedisMQBeanNameConstant.REDISMQ_INNER_MESSAGE_LISTENERCONTAINER;

/**
 * @Author: hzh
 * @Date: 2022/11/4 15:13
 * 自动引入扫描redisMQ注解的配置类
 */
@EnableRedisMQ
@Configuration
public class EnableRedisMQConfiguration {
    @Autowired
    @Qualifier(REDISMQ_INNER_MESSAGE_LISTENERCONTAINER)
    private RedisMessageListenerContainer redismqInnerRedisMessageListenerContainer;
    @Autowired
    private RedisMQProperties redisMqProperties;
    /**
     * redisMQ客户端
     *
     * @return {@link RedisMQProducer}
     */
    @Bean
    public RedisMqClient redisMqClient(RedisMQClientUtil redisMQClientUtil, WorkIdGenerator workIdGenerator
            , RemotingClient remotingClient,RedisMQProducer redisMQProducer) {
        RedisListenerContainerManager redisListenerContainerManager = new RedisListenerContainerManager();
        QueueRebalanceImpl rebalance = new QueueRebalanceImpl(new AllocateMessageQueueAveragely());
        RedisMqClient redisMqClient = new RedisMqClient(redisMQClientUtil, redisListenerContainerManager, rebalance,
                redisMqProperties.getConsumserConfig().getApplicationName(),workIdGenerator,remotingClient,redisMQProducer);
        redisMqClient.setRedisMessageListenerContainer(redismqInnerRedisMessageListenerContainer);
        return redisMqClient;
    }
}
