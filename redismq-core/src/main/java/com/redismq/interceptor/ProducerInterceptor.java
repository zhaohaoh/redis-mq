package com.redismq.interceptor;

import com.redismq.Message;
/**
 * @Author: hzh
 * @Date: 2022/8/9 14:54
 * 生产者回调拦截器
 */
public interface ProducerInterceptor {
    // 发送前回调
    Message beforeSend(Message message);
    // 发送后回调
    void afterSend(Message message);
    // 失败回调
    void onFail(Message message,Exception e);
}
