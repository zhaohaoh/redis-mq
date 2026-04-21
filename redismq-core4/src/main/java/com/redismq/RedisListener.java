package com.redismq;

import java.lang.annotation.*;

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface RedisListener {
    // 队列
    String queue();

    //重试次数 默认配置文件上是0
    int retryMax() default -1;

    int retryInterval() default -1;

    //并发消费数量 默认配置文件上是1
    int concurrency() default -1;

    //最大并发消费数量 默认配置文件上是1
    int maxConcurrency() default -1;

    //ack模型 默认配置文件上是auto
    String ackMode() default "";

    //是否是延时队列 默认false
    boolean delay() default false;

    //路由的key 默认default。如果有一个队列不填写tag那么他默认是default存入map中并且取。实现了默认不配置
    //严禁多个消费者配置相同的topic但是不同的tag。多个消费者对同一个队列的tag必须一致 这点和rocketmq不同 tag的作用一个是业务拆分，一个是共用同一个线程池达到资源复用
    //底层设计上-》一个虚拟队列只会被一个消费者消费。这是设计原则
    String[] tag() default "";

    //名称完全对应的topic  发布订阅使用
    String channelTopic() default "";

    // 虚拟队列数量 默认-1
    int virtual() default -1;

    //队列最大长度 60万
    int queueMaxSize() default -1;
}
