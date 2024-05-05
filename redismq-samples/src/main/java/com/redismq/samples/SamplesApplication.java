package com.redismq.samples;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class SamplesApplication {
 
    public static void main(String[] args) {
        SpringApplication.run(SamplesApplication.class, args);
    }
    
    
    //test 通道连接
//    @Override
//    public void run(String... args) throws Exception {
//      ScheduledThreadPoolExecutor registerThread = new ScheduledThreadPoolExecutor(1);
//        registerThread.scheduleAtFixedRate(new Runnable() {
//            @Override
//            public void run() {
//                Channel channel = nettyClientChannelManager.acquireChannel("192.168.100.89:10520");
//            }
//        },0,5, TimeUnit.SECONDS);
//    }
}
