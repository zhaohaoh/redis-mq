package com.redismq.autoconfigure;

import com.redismq.common.config.GlobalConfigCache;
import com.redismq.common.config.NettyConfig;
import com.redismq.common.connection.RedisClient;
import com.redismq.common.connection.RedisMQClientUtil;
import com.redismq.common.connection.RedisMQServerUtil;
import com.redismq.common.connection.RedissonAdapter;
import com.redismq.common.constant.RedisMQConstant;
import com.redismq.common.util.ServerManager;
import com.redismq.config.RedisProperties;
import com.redismq.core.RedisMQProducer;
import com.redismq.id.WorkIdGenerator;
import com.redismq.interceptor.ProducerInterceptor;
import com.redismq.queue.QueueManager;
import com.redismq.rpc.client.RemotingClient;
import com.redismq.utils.RedisMQTemplate;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;


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

    @Autowired(required = false)
    private List<ProducerInterceptor> producerInterceptors;
 
    
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
    public RedisClient redisClient() {
        RedissonClient redissonClient = RedissonMQClient.redisMQRedisson(redisProperties);
        RedisClient redisClient = new RedissonAdapter(redissonClient);
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
        GlobalConfigCache.CONSUMER_CONFIG=redisMqProperties.getConsumserConfig();
    }
}
