package com.redismq;

import com.redismq.Message;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LocalMessageManager {
    /**
     * 本地消息表
     */
    public static final Map<String, Message> LOCAL_MESSAGES = new ConcurrentHashMap<>();


}
