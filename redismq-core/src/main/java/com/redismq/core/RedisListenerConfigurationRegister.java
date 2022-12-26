package com.redismq.core;


import com.redismq.RedisMqAnnotationBeanPostProcessor;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.context.annotation.ImportBeanDefinitionRegistrar;
import org.springframework.core.type.AnnotationMetadata;

/**
 * @author hzh
 * @date 2021/8/10
 * 注册类 @Import引入
 */
public class RedisListenerConfigurationRegister implements ImportBeanDefinitionRegistrar  {


    @Override
    public void registerBeanDefinitions(AnnotationMetadata importingClassMetadata, BeanDefinitionRegistry registry) {
        registry.registerBeanDefinition("RedisMqAnnotationBeanPostProcessor",
                new RootBeanDefinition(RedisMqAnnotationBeanPostProcessor.class));
    }


}
