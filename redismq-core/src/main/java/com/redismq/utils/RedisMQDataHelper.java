package com.redismq.utils;


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
