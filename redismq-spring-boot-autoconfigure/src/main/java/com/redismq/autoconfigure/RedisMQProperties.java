package com.redismq.autoconfigure;

import com.redismq.common.config.ConsumserConfig;
import com.redismq.common.config.GlobalConfig;
import com.redismq.common.config.GlobalStoreConfig;
import com.redismq.common.config.NettyConfig;
import com.redismq.common.config.ProducerConfig;
import com.redismq.common.config.QueueConfig;
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
     * redis属性
     */
    @NestedConfigurationProperty
    private RedisProperties redisProperties;
    
    @NestedConfigurationProperty
    private ConsumserConfig consumserConfig = new ConsumserConfig();
    
    @NestedConfigurationProperty
    private GlobalConfig globalConfig = new GlobalConfig();
    
    @NestedConfigurationProperty
    private QueueConfig queueConfig = new QueueConfig();
    
    @NestedConfigurationProperty
    private NettyConfig nettyConfig = new NettyConfig();
    
    @NestedConfigurationProperty
    private ProducerConfig producerConfig = new ProducerConfig();
    
    @NestedConfigurationProperty
    private GlobalStoreConfig storeConfig = new GlobalStoreConfig();
}
