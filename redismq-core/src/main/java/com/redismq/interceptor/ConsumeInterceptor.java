package com.redismq.interceptor;

import com.redismq.Message;
/**
 * @Author: hzh
 * @Date: 2022/8/9 15:15
 */
public interface ConsumeInterceptor {
    // 消费前回调
    Message beforeConsume(Message message);
    // 消费后
    Message afterConsume(Message message);
    // 失败回调
    void onFail(Message message,Exception e);
}
