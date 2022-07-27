package com.redismq;

import java.lang.annotation.*;

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface RedisListener {
    //队列名
    String queue() default "";

    //重试次数 默认配置文件上是0
    int retryMax() default -1;

    //并发消费数量 默认配置文件上是1
    int concurrency() default -1;

    //最大并发消费数量 默认配置文件上是1
    int maxConcurrency() default -1;

    //ack模型 默认配置文件上是auto
    String ackMode() default "";

    //是否是延时队列 默认false
    boolean delay() default false;

    //路由的key 默认default。如果有一个队列不填写tag那么他默认是default存入map中并且取。实现了默认不配置队列也有routingKey
    String tag() default "";

    //名称完全对应的topic  发布订阅使用
    String topic() default "";

    //名称完全对应的topic  发布订阅使用
    int virtual() default -1;

    int queueMaxSize() default 10000;
}
