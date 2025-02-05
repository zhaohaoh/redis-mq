//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package com.enable;

import org.redisson.api.RedissonClient;
import org.redisson.spring.data.connection.RedissonConnectionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;

@Configuration
public class RedissonFactoryAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean({RedisConnectionFactory.class})
    @ConditionalOnProperty(name = "spring.redismq.client.client-type", havingValue = "redisson", matchIfMissing = false)
    public RedissonConnectionFactory redissonConnectionFactory(
            @Autowired @Qualifier(value = "redisMQRedisson")
            RedissonClient redisson) {
        return new RedissonConnectionFactory(redisson);
    }
 
}
