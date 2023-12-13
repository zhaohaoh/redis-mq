package com.redismq.autoconfigure;

import com.redismq.config.RedisConnectionFactoryUtil;
import com.redismq.config.RedisProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.ThreadPoolExecutor;

import static com.redismq.constant.RedisMQBeanNameConstant.REDISMQ_INNER_MESSAGE_LISTENERCONTAINER;
import static com.redismq.constant.RedisMQBeanNameConstant.REDISMQ_MESSAGE_LISTENERCONTAINER;
import static com.redismq.constant.RedisMQBeanNameConstant.REDISMQ_REDIS_TEMPLATE;

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
    public StringRedisTemplate redisMQRedisTemplate(RedisConnectionFactoryUtil redisConnectionFactoryUtil) {
        StringRedisTemplate  template = new StringRedisTemplate();
        // 配置连接工厂
        RedisConnectionFactory connectionFactory = redisConnectionFactoryUtil.getSingleConnectionFactory();
        template.setConnectionFactory(connectionFactory);
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
