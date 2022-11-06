package com.redismq.autoconfigure;

import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.context.annotation.Configuration;

/**
 * @Author: hzh
 * @Date: 2022/11/4 15:13
 * 自动引入扫描redisMQ注解的配置类
 */
@Configuration
@EnableRedisMQ
@AutoConfigureAfter(RedisMQAutoConfiguration.class)
public class EnableRedisMQConfiguration {
}
