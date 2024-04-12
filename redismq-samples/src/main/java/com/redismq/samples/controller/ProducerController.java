package com.redismq.samples.controller;

import com.redismq.connection.RedisClient;
import com.redismq.samples.consumer.JavaBean;
import com.redismq.samples.consumer.MakeMappingFile;
import com.redismq.utils.RedisMQTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Date;

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
        JavaBean javaBean = new JavaBean();
        javaBean.setA("ff");
        javaBean.setB(222);
        redisMQTemplate.sendTimingMessage(javaBean, "delaytest1", System.currentTimeMillis()+ Duration.ofSeconds(11).toMillis());
    }
    
    /**
     * 发送普通消息
     */
    @PostMapping("sendMessage")
    @Transactional
    public void sendMessage() {
        String[] names = {"地震震中位置及30日内地震分布图", "地震震区构造背景图", "地震震区历史地震与断层分布图", "地震震中距分布图", "地震震区台站分布图", "地震震区工业平台分布图", "GNSS站点分布图"};
        for (int i = 0; i < names.length; i++) {
            Date date = new Date();
            MakeMappingFile makeMappingFile = new MakeMappingFile();
            makeMappingFile.setName(names[i]);
            makeMappingFile.setCode("0"+i);
            makeMappingFile.setFileType("img");
            makeMappingFile.setCreateTime(date);
            makeMappingFile.setUpdateTime(date);
            makeMappingFile.setModule("earthquake");
            makeMappingFile.setDataId(1L);
            makeMappingFile.setConfig("");
            redisMQTemplate.sendMessage(makeMappingFile, "makeMappingQueue");
        }
        
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
