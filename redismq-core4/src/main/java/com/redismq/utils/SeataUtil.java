package com.redismq.utils;

import io.seata.tm.api.transaction.TransactionHookAdapter;
import io.seata.tm.api.transaction.TransactionHookManager;

public class SeataUtil {
    public void registerHook(Runnable runnable) {
        TransactionHookAdapter adapter = new TransactionHookAdapter() {
            @Override
            public void afterCommit() {
                runnable.run();
            }
        };
        //seata事务提交后执行的方法
        TransactionHookManager.registerHook(adapter);
    }
}
