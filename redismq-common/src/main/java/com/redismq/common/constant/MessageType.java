package com.redismq.common.constant;

public interface MessageType {
    /**
     * 消息
     */
    Integer CREATE_MESSAGE = 1;
    /**
     * 消息发送成功
     */
    Integer SEND_MESSAGE_SUCCESS = 2;
    /**
     * 消息发送成功
     */
    Integer SEND_MESSAGE_FAIL = 3;
    /**
     * 消息
     */
    Integer BATCH_MESSAGE = 10;
}
