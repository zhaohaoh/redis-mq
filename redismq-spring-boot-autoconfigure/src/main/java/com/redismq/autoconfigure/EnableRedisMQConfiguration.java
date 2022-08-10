package com.redismq.autoconfigure;

import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableRedisMQ
@AutoConfigureAfter(RedisMQAutoConfiguration.class)
public class EnableRedisMQConfiguration {
}
