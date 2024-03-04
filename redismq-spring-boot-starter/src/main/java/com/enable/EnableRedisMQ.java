package com.enable;


import com.redismq.core.RedisListenerConfigurationRegister;
import org.springframework.context.annotation.Import;

import java.lang.annotation.*;
/**
 * @Author: hzh
 * @Date: 2022/11/4 15:12
 * 启动redisMQ
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Import(RedisListenerConfigurationRegister.class)
public @interface EnableRedisMQ {
}
