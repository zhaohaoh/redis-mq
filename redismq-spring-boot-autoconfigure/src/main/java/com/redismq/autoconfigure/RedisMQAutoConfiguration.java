package com.redismq.autoconfigure;

import com.redismq.config.GlobalConfigCache;
import com.redismq.config.RedisProperties;
import com.redismq.connection.RedisClient;
import com.redismq.connection.RedisTemplateAdapter;
import com.redismq.constant.RedisMQConstant;
import com.redismq.core.RedisListenerContainerManager;
import com.redismq.core.RedisMQProducer;
import com.redismq.core.RedisMqClient;
import com.redismq.interceptor.ProducerInterceptor;
import com.redismq.queue.QueueManager;
import com.redismq.rebalance.AllocateMessageQueueAveragely;
import com.redismq.rebalance.RebalanceImpl;
import com.redismq.utils.RedisMQTemplate;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;

import javax.annotation.Resource;
import java.util.List;

import static com.redismq.constant.RedisMQBeanNameConstant.REDISMQ_INNER_MESSAGE_LISTENERCONTAINER;
import static com.redismq.constant.RedisMQBeanNameConstant.REDISMQ_REDIS_TEMPLATE;

/**
 * @author hzh
 * RedisMQ自动配置类
 */
@Configuration
@EnableConfigurationProperties({RedisMQProperties.class, RedisProperties.class})
public class RedisMQAutoConfiguration implements InitializingBean {
    @Autowired
    private RedisMQProperties redisMqProperties;
    @Autowired
    private RedisProperties redisProperties;
    @Resource(name = REDISMQ_INNER_MESSAGE_LISTENERCONTAINER)
    private RedisMessageListenerContainer redismqInnerRedisMessageListenerContainer;
    @Autowired(required = false)
    private List<ProducerInterceptor> producerInterceptors;


    /**
      * redisMQ客户端
      *
      * @return {@link RedisMQProducer}
    */
    @Bean
    public RedisMqClient redisMqClient(RedisClient redisClient) {
        RedisListenerContainerManager redisListenerContainerManager = new RedisListenerContainerManager();
        RebalanceImpl rebalance = new RebalanceImpl(new AllocateMessageQueueAveragely());
        RedisMqClient redisMqClient = new RedisMqClient(redisClient, redisListenerContainerManager, rebalance);
        redisMqClient.setRedisMessageListenerContainer(redismqInnerRedisMessageListenerContainer);
        return redisMqClient;
    }


     /**
       * redis客户端
       *
       * @return {@link RedisMQProducer}
     */
    @Bean
    public RedisClient redisClient(@Qualifier(REDISMQ_REDIS_TEMPLATE) RedisTemplate<String, Object> redisMQRedisTemplate) {
        RedisClient redisClient = new RedisTemplateAdapter(redisMQRedisTemplate);
        return redisClient;
    }


    /**
     * 返回redisMQ操作模板
     *
     * @return {@link RedisMQTemplate}
     */
    @Bean
    public RedisMQTemplate redisMQTemplate(RedisClient redisClient) {
        RedisMQProducer redisMQProducer = new RedisMQProducer(redisClient);
        redisMQProducer.setProducerInterceptors(producerInterceptors);
        return new RedisMQTemplate(redisMQProducer);
    }

    /**
     * 初始化执行
     */
    @Override
    public void afterPropertiesSet() throws Exception {
        QueueManager.VIRTUAL_QUEUES_NUM = redisMqProperties.getQueueConfig().getVirtual();
        RedisMQConstant.GROUP = redisMqProperties.getGroup();
        GlobalConfigCache.GLOBAL_CONFIG = redisMqProperties.getGlobalConfig();
        GlobalConfigCache.QUEUE_CONFIG = redisMqProperties.getQueueConfig();
    }
}
