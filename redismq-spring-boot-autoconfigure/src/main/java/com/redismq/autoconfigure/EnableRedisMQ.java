package com.redismq.autoconfigure;


import com.redismq.core.RedisListenerConfigurationRegister;
import org.springframework.context.annotation.Import;

import java.lang.annotation.*;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Import(RedisListenerConfigurationRegister.class)
public @interface EnableRedisMQ {
}
