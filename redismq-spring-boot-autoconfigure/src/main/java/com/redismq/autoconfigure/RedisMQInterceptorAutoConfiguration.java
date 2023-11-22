package com.redismq.autoconfigure;

import com.redismq.connection.RedisMQClientUtil;
import com.redismq.interceptor.ConsumeInterceptor;
import com.redismq.interceptor.DefaultProducerInterceptor;
import com.redismq.interceptor.ProducerInterceptor;
import com.redismq.interceptor.RedisDeadQueueHandleInterceptor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * redisMQ 拦截器自动装置
 *
 * @author hzh
 * @date 2022/12/30
 */
@Configuration(proxyBeanMethods = false)
public class RedisMQInterceptorAutoConfiguration {

    /**
     * 默认的死信队列拦截器
     *
     * @param redisMQClientUtil  客户端工具类
     * @return {@link RedisDeadQueueHandleInterceptor}
     */
    @Bean
    @ConditionalOnProperty(value = "spring.redismq.dead-letter-queue.enable", havingValue = "true")
    @ConditionalOnMissingBean(ConsumeInterceptor.class)
    public ConsumeInterceptor redisDeadQueueHandleInterceptor(RedisMQClientUtil redisMQClientUtil) {
        return new RedisDeadQueueHandleInterceptor(redisMQClientUtil);
    }

    /**
     * 默认的生产者拦截器-打印日志
     *
     * @return {@link ProducerInterceptor}
     */
    @Bean
    @ConditionalOnMissingBean(ProducerInterceptor.class)
    public ProducerInterceptor producerInterceptor() {
        return new DefaultProducerInterceptor();
    }
}
