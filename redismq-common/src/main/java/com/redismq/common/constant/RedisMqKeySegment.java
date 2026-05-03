package com.redismq.common.constant;

public enum RedisMqKeySegment {
    QUEUES("queues"),
    GROUPS("groups"),
    CLIENTS("clients"),
    SERVERS("servers"),
    TOPIC("topic"),
    PULL("pull"),
    SERVER("server"),
    REBALANCE("rebalance"),
    LOCK("lock"),
    SEQUENCE("sequence"),
    SEND("send"),
    WORKER_IDS("worker-ids"),
    GROUP("group"),
    MESSAGES("messages"),
    OFFSET("offset"),
    DEAD("dead"),
    BODY("body"),
    VQUEUES("vqueues");

    private final String value;

    RedisMqKeySegment(String value) {
        this.value = value;
    }

    public String value() {
        return value;
    }
}
