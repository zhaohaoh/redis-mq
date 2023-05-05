package com.redismq.core;

import io.seata.tm.api.transaction.TransactionHookAdapter;
import io.seata.tm.api.transaction.TransactionHookManager;

/**
 * @Author: hzh
 * @Date: 2022/5/19 15:46
 * redismq生产者
 */
public class RedisMQProducerI {
    public void a() {
        TransactionHookAdapter adapter = new TransactionHookAdapter() {
            @Override
            public void afterCommit() {
                System.out.println("11");
            }
        };
        //seata事务提交后执行的方法
        TransactionHookManager.registerHook(adapter);
    }


}
