package com.redismq.samples.interceptor;

import com.redismq.Message;
import com.redismq.interceptor.ProducerInterceptor;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class ProducerInterceptorImpl implements ProducerInterceptor {
    @Override
    public void afterSend(List<Message> messages) {
        System.out.println("消息发送后回调"+messages);
    }

    @Override
    public void onFail(List<Message> messages, Exception e) {
        System.out.println(e);
    }
}
