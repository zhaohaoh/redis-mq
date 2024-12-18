package com.redismq.samples.controller;

import com.redismq.common.connection.RedisClient;
import com.redismq.samples.consumer.JavaBean;
import com.redismq.utils.RedisMQTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

/**
 * @Author: hzh
 * @Date: 2022/12/26 17:54
 * 生产消息的例子
 */
@RestController
@RequestMapping("producer")
public class ProducerController {
    @Autowired
    private RedisMQTemplate redisMQTemplate;
    @Autowired
    private RedisClient redisClient;
  
    @Autowired
    private StringRedisTemplate stringRedisTemplate;
    /**
     * 发送延迟消息
     */
    @PostMapping("sendDelayMessage")
    public void sendDelayMessage() {
//        for (int i = 0; i < 100; i++) {
//            JavaBean javaBean = new JavaBean();
//            javaBean.setA("ff");
//            javaBean.setB(222);
//            redisMQTemplate.sendTimingMessage(javaBean, "delaytest1", System.currentTimeMillis()+Duration.ofSeconds(1111).toMillis());
//        }
//        long millis = System.currentTimeMillis()+  Duration.ofSeconds(30).toMillis();
        for (int i = 0; i < 1000; i++) {
            JavaBean javaBean = new JavaBean();
            javaBean.setA("ff");
            javaBean.setB(222);
           
            redisMQTemplate.sendTimingMessage(javaBean, "delaytest1", 1734508140000L);
        }
      
    }
    
    @PostMapping("sendDelayMessage2")
    public void sendDelayMessage2() {
        //        for (int i = 0; i < 100; i++) {
        //            JavaBean javaBean = new JavaBean();
        //            javaBean.setA("ff");
        //            javaBean.setB(222);
        //            redisMQTemplate.sendTimingMessage(javaBean, "delaytest1", System.currentTimeMillis()+Duration.ofSeconds(1111).toMillis());
        //        }
        JavaBean javaBean = new JavaBean();
        javaBean.setA("ff");
        javaBean.setB(222);
        redisMQTemplate.sendTimingMessage(javaBean, "delaytest1", System.currentTimeMillis()+ Duration.ofSeconds(1).toMillis());
    }
    
    /**
     * 发送普通消息
     */
    @PostMapping("sendMessage")
    public void sendMessage() {
        JavaBean javaBean = new JavaBean();
        javaBean.setA("ff");
        javaBean.setB(222);
        redisMQTemplate.sendMessage(javaBean, "test1");
    }
    /**
     * 发送顺序消息
     */
    @PostMapping("sendOrderMessage")
    public void sendOrderMessage() {
        redisMQTemplate.sendMessage("顺序消息消费", "order");
    }

    /**
     * 发送定时消费消息 带tag
     */
    @PostMapping("sendTimingMessage")
    public void sendTimingMessage() {
        JavaBean javaBean = new JavaBean();
        javaBean.setA("ff");
        javaBean.setB(222);
        LocalDateTime time = LocalDateTime.of(2023, 12, 14, 14, 20, 30);
        long l = time.toInstant(ZoneOffset.ofHours(8)).toEpochMilli();
        redisMQTemplate.sendTimingMessage(javaBean, "time", "bussiness1", l);
    }


    /**
     * 发送定时消费消息 带tag
     */
    @PostMapping("sendMultiTagMessage")
    public void sendMultiTagMessage() {
        for (int i = 0; i < 100; i++) {
            redisMQTemplate.sendMessage("多个标签同一Queue消息消费1", "MultiTag", "bussiness1");
            redisMQTemplate.sendMessage("多个标签同一Queue消息消费2", "MultiTag", "bussiness2");
            redisMQTemplate.sendMessage("多个标签同一Queue消息消费1", "MultiTag", "bussiness1");
            redisMQTemplate.sendMessage("多个标签同一Queue消息消费2", "MultiTag", "bussiness2");
            redisMQTemplate.sendMessage("多个标签同一Queue消息消费1", "MultiTag", "bussiness1");
            redisMQTemplate.sendMessage("多个标签同一Queue消息消费2", "MultiTag", "bussiness2");
            redisMQTemplate.sendMessage("多个标签同一Queue消息消费1", "MultiTag", "bussiness1");
            redisMQTemplate.sendMessage("多个标签同一Queue消息消费2", "MultiTag", "bussiness2");
        }
    }
 
}
