package com.redismq.samples.consumer;

import com.redismq.queue.Queue;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class QueueConfig {
    @Bean
    public Queue queue(){
        Queue queue = new Queue();
        queue.setQueueName("delaytest1");
        queue.setDelayState(true);
        queue.setQueueMaxSize(100000);
        return queue;
    }
}
