package com.redismq.samples.consumer;

import com.redismq.Message;
import com.redismq.RedisListener;
import org.springframework.stereotype.Component;

/**
 * @Author: hzh
 * @Date: 2022/12/26 17:54
 * 消费者简单案例
 */
@Component
public class SamplesConsumer  {


//    /**
//     * delaytest1消费延时队列
//     */
    @RedisListener(queue = "delaytest1", virtual = 4,delay = true,maxConcurrency = 64,concurrency = 8,retryMax = 5)
    public void delaytest1(JavaBean test) {
        System.out.println(test);
        throw new RuntimeException();
    }

    /**
     * 普通消息消费
     */
    @RedisListener(queue = "earthquakeTrigger")
    public void test1(JavaBean javaBean) throws InterruptedException {
       
        Thread.sleep(3000L);
        System.out.println(javaBean);
    }
    
    /**
     * 普通消息消费
     */
    @RedisListener(queue = "makeMappingQueue")
    public void makeMappingQueue(JavaBean javaBean) throws InterruptedException {
        
        Thread.sleep(3000L);
        System.out.println(javaBean);
    }

//    /**
//     * 顺序消息消费  虚拟队列，消费者线程都设置为1即可保证顺序
//     */
//    @RedisListener(queue = "order", virtual = 1, concurrency = 1, maxConcurrency = 1)
//    public void order(Message message) {
//        System.out.println(message);
//        throw new RuntimeException();
//    }
//
    @RedisListener(queue = "time",tag = "bussiness1",delay = true)
    public void time(Message message) {
        JavaBean javaBean = message.parseJavaBean(JavaBean.class);
        System.out.println(javaBean);
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
