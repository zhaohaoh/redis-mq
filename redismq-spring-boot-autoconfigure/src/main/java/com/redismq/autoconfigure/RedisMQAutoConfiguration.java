package com.redismq.autoconfigure;

import com.redismq.common.config.GlobalConfigCache;
import com.redismq.common.config.NettyConfig;
import com.redismq.common.connection.RedisClient;
import com.redismq.common.connection.RedisMQClientUtil;
import com.redismq.common.connection.RedisMQServerUtil;
import com.redismq.common.connection.StringRedisTemplateAdapter;
import com.redismq.common.constant.RedisMQConstant;
import com.redismq.common.util.ServerManager;
import com.redismq.config.RedisConnectionFactoryUtil;
import com.redismq.config.RedisProperties;
import com.redismq.core.RedisListenerContainerManager;
import com.redismq.core.RedisMQProducer;
import com.redismq.core.RedisMqClient;
import com.redismq.id.WorkIdGenerator;
import com.redismq.interceptor.ProducerInterceptor;
import com.redismq.queue.QueueManager;
import com.redismq.rebalance.AllocateMessageQueueAveragely;
import com.redismq.rebalance.RebalanceImpl;
import com.redismq.rpc.client.RemotingClient;
import com.redismq.utils.RedisMQTemplate;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;

import java.util.List;

import static com.redismq.common.constant.RedisMQBeanNameConstant.REDISMQ_INNER_MESSAGE_LISTENERCONTAINER;


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
    @Qualifier(REDISMQ_INNER_MESSAGE_LISTENERCONTAINER)
    private RedisMessageListenerContainer redismqInnerRedisMessageListenerContainer;
    @Autowired(required = false)
    private List<ProducerInterceptor> producerInterceptors;
   


    /**
      * redisMQ客户端
      *
      * @return {@link RedisMQProducer}
    */
    @Bean
    public RedisMqClient redisMqClient(RedisMQClientUtil redisMQClientUtil, WorkIdGenerator workIdGenerator) {
        RedisListenerContainerManager redisListenerContainerManager = new RedisListenerContainerManager();
        RebalanceImpl rebalance = new RebalanceImpl(new AllocateMessageQueueAveragely());
        RedisMqClient redisMqClient = new RedisMqClient(redisMQClientUtil, redisListenerContainerManager, rebalance,
                redisMqProperties.getApplicationName(),workIdGenerator);
        redisMqClient.setRedisMessageListenerContainer(redismqInnerRedisMessageListenerContainer);
        return redisMqClient;
    }
    
    @Bean
    public RedisMQClientUtil redisMQClientUtil(RedisClient redisClient) {
        return new RedisMQClientUtil(redisClient);
    }
    
    @Bean
    public RedisMQServerUtil redisMQServerUtil(RedisClient redisClient) {
        return new RedisMQServerUtil(redisClient);
    }

     /**
       * redis客户端
       *
       * @return {@link RedisMQProducer}
     */
    @Bean
    public RedisClient redisClient(RedisConnectionFactoryUtil redisConnectionFactoryUtil) {
        StringRedisTemplate template = new StringRedisTemplate();
        // 配置连接工厂
        RedisConnectionFactory connectionFactory = redisConnectionFactoryUtil.getSingleConnectionFactory();
        template.setConnectionFactory(connectionFactory);
        template.afterPropertiesSet();
        RedisClient redisClient = new StringRedisTemplateAdapter(template);
        return redisClient;
    }
    
    @Bean
    public WorkIdGenerator workIdGenerator(RedisClient redisClient) {
        return new WorkIdGenerator(redisClient,redisMqProperties.getGlobalConfig().getMaxWorkerIdBits());
    }


    /**
     * 返回redisMQ操作模板
     *
     * @return {@link RedisMQTemplate}
     */
    @Bean
    public RedisMQTemplate redisMQTemplate(RedisMQProducer redisMQProducer) {
        redisMQProducer.setProducerInterceptors(producerInterceptors);
        return new RedisMQTemplate(redisMQProducer);
    }
    
    @Bean
    public RedisMQProducer redisMQProducer(RedisMQClientUtil redisMQClientUtil,@Autowired(required = false) RemotingClient remotingClient) {
        return new RedisMQProducer(redisMQClientUtil,remotingClient);
    }
    
    @Bean
    public QueueManager queueManager() {
        return new QueueManager();
    }
    @Bean
    public ServerManager serverManager(){
        return  new ServerManager();
    }

    /**
     * 初始化执行
     */
    @Override
    public void afterPropertiesSet() throws Exception {
        QueueManager.VIRTUAL_QUEUES_NUM = redisMqProperties.getQueueConfig().getVirtual();
        RedisMQConstant.NAMESPACE = redisMqProperties.getNamespace();
        GlobalConfigCache.GLOBAL_CONFIG = redisMqProperties.getGlobalConfig();
        GlobalConfigCache.QUEUE_CONFIG = redisMqProperties.getQueueConfig();
        GlobalConfigCache.PRODUCER_CONFIG = redisMqProperties.getProducerConfig();
        GlobalConfigCache.GLOBAL_STORE_CONFIG = redisMqProperties.getStoreConfig();
        NettyConfig nettyConfig = redisMqProperties.getNettyConfig();
        nettyConfig.init();
        GlobalConfigCache.NETTY_CONFIG = nettyConfig;
    }
}
