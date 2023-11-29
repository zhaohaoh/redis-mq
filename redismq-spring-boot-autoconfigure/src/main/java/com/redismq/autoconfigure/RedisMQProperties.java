package com.redismq.autoconfigure;

import com.redismq.config.GlobalConfig;
import com.redismq.config.QueueConfig;
import com.redismq.config.RedisProperties;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;

/**
 * @Author: hzh
 * @Date: 2022/11/4 15:29
 */
@Data
@ConfigurationProperties(prefix = RedisMQProperties.PREFIX)
public class RedisMQProperties {
    
    public static final String PREFIX = "spring.redismq";
    
    /**
     * 环境隔离统一后缀
     */
    private String namespace = "";
    
    
    /**
     * 应用名
     */
    private String applicationName = "redisMQ-Client";
    
    /**
     * redis属性
     */
    private RedisProperties redisProperties;
    
    
    @NestedConfigurationProperty
    private GlobalConfig globalConfig = new GlobalConfig();
    
    @NestedConfigurationProperty
    private QueueConfig queueConfig = new QueueConfig();
}
