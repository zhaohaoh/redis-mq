package com.redismq.samples.consumer;

import com.redismq.RedisListener;
import com.redismq.common.pojo.Message;
import com.redismq.utils.RedisMQTemplate;
import lombok.SneakyThrows;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @Author: hzh
 * @Date: 2022/12/26 17:54
 * 消费者简单案例
 */
@Component
public class SamplesConsumer  {
    @Autowired
    private RedisMQTemplate redisMQTemplate;
    Map<String,Object> map =new ConcurrentHashMap<>();

    /**
     * delaytest1消费延时队列
     */
    @SneakyThrows
    @RedisListener(queue = "delaytest1",delay = true,maxConcurrency = 200,concurrency = 200,retryMax = 16)
    public void delaytest1(Message message) {
        Long executeTime = message.getExecuteTime();
        Long offset = message.getOffset();
        long l = System.currentTimeMillis();
        long diff = l - executeTime;
        Object object = map.get(message.getId());
        Thread.sleep(1000);
        if (object!=null){
            System.out.println(Thread.currentThread().getName()+" 重复消费"+message.getId()+"偏移量"+offset+"应该消费的时间:"+executeTime+"实际消费的时间:"+l +"差值:"+diff);
        }
        map.put(message.getId(), "fff");
        System.out.println(message.getId()+"偏移量"+offset+"应该消费的时间:"+executeTime+"实际消费的时间:"+l +"差值:"+diff);
    }
//
//    /**
//     * 普通消息消费
//     */
//    @RedisListener(queue = "earthquakeTrigger",virtual = 3)
//    public void test1(Message data) throws InterruptedException {
//        Object body = data.getBody();
//        Thread.sleep(1500L);
//        System.out.println(data.getOffset());
//    }
//
////    /**
////     * 顺序消息消费  虚拟队列，消费者线程都设置为1即可保证顺序
////     */
////    @RedisListener(queue = "order", virtual = 1, concurrency = 1, maxConcurrency = 1)
////    public void order(Message message) {
////        System.out.println(message);
////        throw new RuntimeException();
////    }
////
//    @RedisListener(queue = "time",tag = "bussiness1",delay = true)
//    public void time(Message message) {
//        JavaBean javaBean = message.parseJavaBean(JavaBean.class);
//        System.out.println(javaBean);
//        System.out.println(message);
//    }
//
//    @RedisListener(queue = "test1")
//    public void aa(Message message) {
//        JavaBean javaBean = message.parseJavaBean(JavaBean.class);
//        System.out.println(javaBean);
//        System.out.println(message);
//    }
//
    
//    @RedisListener(queue = "test1",maxConcurrency = 64,concurrency = 8,retryMax = 5)
//    public void test1(JavaBean test) {
//        redisMQTemplate.sendMessage(test,"test2");
//    }
//
    @RedisListener(queue = "test1",maxConcurrency = 64,concurrency = 8,retryMax = 5,virtual = 2)
    public void test2(Message message) throws InterruptedException {
        Long executeTime = message.getExecuteTime();
        Long offset = message.getOffset();
        long l = System.currentTimeMillis();
        long diff = l - executeTime;
        String id = message.getId();
        Thread.sleep(1000);
        Object object = map.get(id);
        if (object!=null){
            System.out.println(Thread.currentThread().getName()+" 重复消费"+message.getId()+"偏移量"+offset+"应该消费的时间:"+executeTime+"实际消费的时间:"+l +"差值:"+diff);
        }
        System.out.println(message);
    }
    
//
//
//    /**
//     * 多标签同topic消费，会由同一个线程池进行消费  严禁不同消费者配置不同的tag！这点和rocketmq不同
//     * tag的作用一个是业务拆分，一个是共用同一个线程池达到资源复用
//     *
//     * @param message 消息
//     */
//    @RedisListener(queue = "MultiTag",tag = "bussiness1")
//    public void multiTag1(Message message) {
//        //模拟业务消费
//        try {
//            Thread.sleep(1000L);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//        String name = Thread.currentThread().getName();
//        System.out.println(name+message);
//    }
//
//    @RedisListener(queue = "MultiTag",tag = "bussiness2")
//    public void multiTag2(Message message) {
//        //模拟业务消费
//        try {
//            Thread.sleep(1000L);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//        String name = Thread.currentThread().getName();
//        System.out.println(name+message);
//    }
//
//
//    @RedisListener(queue = "admintest")
//    public void admintest(JavaBean javaBean) {
//        System.out.println(javaBean);
//    }
//
//    @RedisListener(queue = "fsd")
//    public void fgdsf(String javaBean) {
//        System.out.println(javaBean);
//    }
//
//    @RedisListener(queue = "fsdfsd")
//    public void sdfdaas(JavaBean javaBean) {
//        System.out.println(javaBean);
//    }
//    @RedisListener(queue = "fsdrrr")
//    public void sdfds(JavaBean javaBean) {
//        System.out.println(javaBean);
//    }
//    @RedisListener(queue = "ggggg")
//    public void sdf(JavaBean javaBean) {
//        System.out.println(javaBean);
//    }
//    @RedisListener(queue = "gggdsf")
//    public void aa(JavaBean javaBean) {
//        System.out.println(javaBean);
//    }
//    @RedisListener(queue = "gdfhfdhdf")
//    public void gdfgfdgdf(JavaBean javaBean) {
//        System.out.println(javaBean);
//    }
//
}
