package com.redismq.autoconfigure;

import com.redismq.common.connection.RedisMQClientUtil;
import com.redismq.core.RedisListenerContainerManager;
import com.redismq.core.RedisMQProducer;
import com.redismq.core.RedisMqClient;
import com.redismq.id.WorkIdGenerator;
import com.redismq.rebalance.AllocateMessageQueueAveragely;
import com.redismq.rebalance.QueueRebalanceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;

import static com.redismq.common.constant.RedisMQBeanNameConstant.REDISMQ_INNER_MESSAGE_LISTENERCONTAINER;

@Configuration
public class RedisMQClientAutoConfiguration {
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
    public RedisMqClient redisMqClient(RedisMQClientUtil redisMQClientUtil, WorkIdGenerator workIdGenerator) {
        RedisListenerContainerManager redisListenerContainerManager = new RedisListenerContainerManager();
        QueueRebalanceImpl rebalance = new QueueRebalanceImpl(new AllocateMessageQueueAveragely());
        RedisMqClient redisMqClient = new RedisMqClient(redisMQClientUtil, redisListenerContainerManager, rebalance,
                redisMqProperties.getConsumserConfig().getApplicationName(),workIdGenerator);
        redisMqClient.setRedisMessageListenerContainer(redismqInnerRedisMessageListenerContainer);
        return redisMqClient;
    }
}
