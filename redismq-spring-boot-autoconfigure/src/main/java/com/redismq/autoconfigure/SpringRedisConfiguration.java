package com.redismq.autoconfigure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.redismq.config.RedisConnectionFactoryUtil;
import com.redismq.config.RedisProperties;
import com.redismq.utils.RedisMQObjectMapper;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.ThreadPoolExecutor;

import static com.redismq.constant.RedisMQBeanNameConstant.*;

/**
 * 配置springredis  避免循环依赖
 *
 * @author hzh
 * @date 2022/12/26
 */
@Configuration
public class SpringRedisConfiguration {
    /**
     * redisMQ使用的redisTemplate
     *
     * @return {@link RedisTemplate}
     */
    @Bean(REDISMQ_MESSAGE_LISTENERCONTAINER)
    public RedisMessageListenerContainer redisMQMessageListenerContainer(RedisConnectionFactoryUtil redisConnectionFactoryUtil) {
        RedisMessageListenerContainer redisMessageListenerContainer = new RedisMessageListenerContainer();
        redisMessageListenerContainer.setConnectionFactory(redisConnectionFactoryUtil.getSingleConnectionFactory());
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

    @Bean(name = REDISMQ_REDIS_TEMPLATE)
    public RedisTemplate<String, Object> redisMQRedisTemplate(RedisConnectionFactoryUtil redisConnectionFactoryUtil) {
        RedisTemplate<String, Object> template = new RedisTemplate<>();
        // 配置连接工厂
        RedisConnectionFactory connectionFactory = redisConnectionFactoryUtil.getSingleConnectionFactory();
        template.setConnectionFactory(connectionFactory);

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

    /**
     * 连接工厂工具类,上面用的都是构造方法注入,这里的redisProperties也得用构造方法注入.spring早构造方法注入的时候还没有解析@Autowired,
     * 此时Autowired是对象都是null
     *
     * @return {@link RedisConnectionFactoryUtil}
     */
    @Bean
    public RedisConnectionFactoryUtil redisConnectionFactoryUtil(RedisProperties redisProperties) {
        return new RedisConnectionFactoryUtil(redisProperties);
    }

    @Bean(REDISMQ_INNER_MESSAGE_LISTENERCONTAINER)
    public RedisMessageListenerContainer redismqInnerRedisMessageListenerContainer(RedisConnectionFactoryUtil redisConnectionFactoryUtil) {
        RedisMessageListenerContainer redisMessageListenerContainer = new RedisMessageListenerContainer();
        redisMessageListenerContainer.setConnectionFactory(redisConnectionFactoryUtil.getSingleConnectionFactory());
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

}
