package com.redismq.constant;

public class QueueConstant {
    //待确认队列
    public static final String DEFAULT_UNACK_QUEUE_PREFIX = "REDISMQ:UNACK:";
    //死信队列
    public static final String DEFAULT_DEAD_QUEUE_PREFIX = "REDISMQ:DEAD:";
    //虚拟队列分隔符
    public static final String SPLITE = ":";
}
