package com.redismq.interceptor;

import com.redismq.Message;

import java.util.List;

/**
 * @Author: hzh
 * @Date: 2022/8/9 14:54
 * 生产者回调拦截器
 */
public interface ProducerInterceptor {

    // 发送前回调
    default void beforeSend(List<Message> messages) {
    }

    // 发送后
    default void afterSend(List<Message> messages) {
    }

    // 失败回调
    default void onFail(List<Message> messages, Exception e) {
    }
}
