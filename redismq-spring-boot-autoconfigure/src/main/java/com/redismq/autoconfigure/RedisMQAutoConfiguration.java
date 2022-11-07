package com.redismq.autoconfigure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.redismq.config.GlobalConfigCache;
import com.redismq.connection.RedisClient;
import com.redismq.connection.RedisTemplateAdapter;
import com.redismq.constant.GlobalConstant;
import com.redismq.constant.RedisMQConstant;
import com.redismq.core.RedisListenerContainerManager;
import com.redismq.core.RedisMQProducer;
import com.redismq.core.RedisMqClient;
import com.redismq.factory.DefaultRedisListenerContainerFactory;
import com.redismq.interceptor.ConsumeInterceptor;
import com.redismq.interceptor.ProducerInterceptor;
import com.redismq.queue.QueueManager;
import com.redismq.rebalance.AllocateMessageQueueAveragely;
import com.redismq.rebalance.RebalanceImpl;
import com.redismq.utils.RedisMQObjectMapper;
import com.redismq.utils.RedisMQTemplate;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.AutoConfigureOrder;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.data.redis.RedisProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import javax.annotation.Resource;
import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @author hzh
 * RedisMQ自动配置类
 */
@AutoConfigureOrder(Ordered.LOWEST_PRECEDENCE)
@Configuration
@EnableConfigurationProperties({RedisMQProperties.class, RedisProperties.class})
public class RedisMQAutoConfiguration implements InitializingBean {
    @Autowired
    private RedisMQProperties redisMqProperties;
    @Autowired
    private RedisProperties redisProperties;
    @Resource(name = "redisMQRedisTemplate")
    private RedisTemplate<String, Object> redisMQRedisTemplate;
    @Autowired
    private RedisClient redisClient;
    @Resource(name = "redismqInnerRedisMessageListenerContainer")
    private RedisMessageListenerContainer redismqInnerRedisMessageListenerContainer;
    @Autowired(required = false)
    private List<ProducerInterceptor> producerInterceptors;
    @Autowired(required = false)
    private List<ConsumeInterceptor> consumeInterceptors;

    /**
     * @return 默认的监听队列容器工厂
     */
    @Bean
    public DefaultRedisListenerContainerFactory redisListenerContainerFactory() {
        DefaultRedisListenerContainerFactory redisListenerContainerFactory = new DefaultRedisListenerContainerFactory();
        redisListenerContainerFactory.setConcurrency(redisMqProperties.getConcurrency());
        redisListenerContainerFactory.setMaxConcurrency(redisMqProperties.getMaxConcurrency());
        redisListenerContainerFactory.setRetryMax(redisMqProperties.getConsumeRetryMax());
        redisListenerContainerFactory.setAckMode(redisMqProperties.getAckMode());
        redisListenerContainerFactory.setRetryInterval(redisMqProperties.getRetryInterval());
        redisListenerContainerFactory.setRedisClient(redisClient);
        redisListenerContainerFactory.setTimeout(redisProperties.getTimeout());
        redisListenerContainerFactory.setConsumeInterceptors(consumeInterceptors);
        return redisListenerContainerFactory;
    }


    /**
     * 返回redisMQ操作模板
     *
     * @return {@link RedisMQTemplate}
     */
    @Bean
    public RedisMQTemplate redisMQTemplate() {
        RedisMQTemplate redisMQTemplate = new RedisMQTemplate(redisMQProducer());
        return redisMQTemplate;
    }

    /**
     * redisMQ生产者
     *
     * @return {@link RedisMQProducer}
     */
    @Bean
    public RedisMQProducer redisMQProducer() {
        RedisMQProducer redisMQProducer = new RedisMQProducer(redisClient);
        redisMQProducer.setProducerInterceptors(producerInterceptors);
        return redisMQProducer;
    }

    /**
     * redisMQ客户端
     *
     * @return {@link RedisMQProducer}
     */
    @Bean
    public RedisMqClient redisMqClient() {
        RedisMqClient redisMqClient = new RedisMqClient(redisClient, redisListenerContainerManager(), rebalance());
        redisMqClient.setRedisMessageListenerContainer(redismqInnerRedisMessageListenerContainer);
        return redisMqClient;
    }

    /**
     * redis客户端
     *
     * @return {@link RedisMQProducer}
     */
    @Bean
    public RedisClient redisClient() {
        RedisClient redisClient = new RedisTemplateAdapter(redisMQRedisTemplate);
        return redisClient;
    }

    /**
     * redisMQ负载均衡器
     *
     * @return {@link RedisMQProducer}
     */
    @Bean
    public RebalanceImpl rebalance() {
        return new RebalanceImpl(new AllocateMessageQueueAveragely());
    }

    /**
     * redisMQ容器管理者
     *
     * @return {@link RedisMQProducer}
     */
    @Bean
    public RedisListenerContainerManager redisListenerContainerManager() {
        RedisListenerContainerManager redisListenerContainerManager = new RedisListenerContainerManager();
        return redisListenerContainerManager;
    }

    /**
     * redisMQ内部使用的发布订阅容器
     *
     * @return {@link RedisMQProducer}
     */
    @Bean("redismqInnerRedisMessageListenerContainer")
    public RedisMessageListenerContainer redismqInnerRedisMessageListenerContainer(RedisConnectionFactory redisConnectionFactory) {
        RedisMessageListenerContainer redisMessageListenerContainer = new RedisMessageListenerContainer();
        redisMessageListenerContainer.setConnectionFactory(redisConnectionFactory);
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        // 设置核心线程数
        executor.setCorePoolSize(5);
        // 设置最大线程数
        executor.setMaxPoolSize(5);
        // 设置队列容量 10万
        executor.setQueueCapacity(100000);
        // 设置线程活跃时间（秒）
        executor.setKeepAliveSeconds(60);
        // 设置默认线程名称
        executor.setThreadNamePrefix("REDISMQ_INNER_LISTENER");
        // 设置拒绝策略
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        executor.initialize();
        redisMessageListenerContainer.setTaskExecutor(executor);
        return redisMessageListenerContainer;
    }

    /**
     * redisMQ默认外部使用的发布订阅
     *
     * @return {@link RedisMessageListenerContainer}
     */
    @Bean("redisMQMessageListenerContainer")
    public RedisMessageListenerContainer redisMQMessageListenerContainer(RedisConnectionFactory redisConnectionFactory) {
        RedisMessageListenerContainer redisMessageListenerContainer = new RedisMessageListenerContainer();
        redisMessageListenerContainer.setConnectionFactory(redisConnectionFactory);
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        // 设置核心线程数
        executor.setCorePoolSize(8);
        // 设置最大线程数
        executor.setMaxPoolSize(8);
        // 设置队列容量
        executor.setQueueCapacity(10000);
        // 设置线程活跃时间（秒）
        executor.setKeepAliveSeconds(60);
        // 设置默认线程名称
        executor.setThreadNamePrefix("redismq-pubsub-");
        // 设置拒绝策略
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        // 等待所有任务结束后再关闭线程池
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.initialize();
        redisMessageListenerContainer.setTaskExecutor(executor);
        return redisMessageListenerContainer;
    }

    /**
     * redisMQ使用的redisTemplate
     *
     * @return {@link RedisTemplate}
     */
    @Bean(name = "redisMQRedisTemplate")
    public RedisTemplate<String, Object> redisTemplate(RedisConnectionFactory redisConnectionFactory) {
        RedisTemplate<String, Object> template = new RedisTemplate<>();
        // 配置连接工厂
        template.setConnectionFactory(redisConnectionFactory);

        //使用Jackson2JsonRedisSerializer来序列化和反序列化redis的value值（默认使用JDK的序列化方式）这种序列化速度中上，明文存储
        Jackson2JsonRedisSerializer<Object> jacksonSeial = new Jackson2JsonRedisSerializer<>(Object.class);
        ObjectMapper mapper = RedisMQObjectMapper.MAPPER;
        jacksonSeial.setObjectMapper(mapper);

        template.setKeySerializer(new StringRedisSerializer());
        // 值采用json序列化
        template.setValueSerializer(jacksonSeial);
        // 设置hash key 和value序列化模式
        template.setHashKeySerializer(new StringRedisSerializer());
        template.setHashValueSerializer(jacksonSeial);
        template.afterPropertiesSet();
        return template;
    }

    @Bean
    @ConditionalOnProperty(value = "spring.redismq.dead-letter-queue.enable", havingValue = "true")
    public RedisDeadQueueHandleInterceptor redisDeadQueueHandleInterceptor() {
        return new RedisDeadQueueHandleInterceptor(redisClient);
    }

    /**
     * 初始化执行
     */
    @Override
    public void afterPropertiesSet() throws Exception {
        QueueManager.VIRTUAL_QUEUES_NUM = redisMqProperties.getVirtual();
        RedisMQConstant.GROUP = redisMqProperties.getGroup();
        GlobalConfigCache.GLOBAL_CONFIG = redisMqProperties.getGlobalConfig();
    }
}
