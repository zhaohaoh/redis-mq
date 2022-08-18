package com.redismq.interceptor;

import com.redismq.Message;

/**
 * @Author: hzh
 * @Date: 2022/8/9 15:15
 */
public interface ConsumeInterceptor {
    // 消费前回调
    default void beforeConsume(Message message) {
    }

    // 消费后
    default void afterConsume(Message message) {

    }

    // 失败回调
    default void onFail(Message message, Exception e) {

    }
}
