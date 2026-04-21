package com.redismq.utils;

/**
 * @Author: hzh
 * @Date: 2022/11/7 11:03
 * 目前主要是用作mq事务后提交的功能
 */
public class RedisMQDataHelper {
    public static final ThreadLocal<Boolean> SEND_AFTER_COMMIT = new ThreadLocal<>();

    public static void sendAfterCommit(boolean sendAfterCommit) {
        SEND_AFTER_COMMIT.set(sendAfterCommit);
    }

    public static Boolean get() {
        Boolean sendAfterCommit = SEND_AFTER_COMMIT.get();
        SEND_AFTER_COMMIT.remove();
        return sendAfterCommit;
    }

    public static void remove() {
        SEND_AFTER_COMMIT.remove();
    }
}
