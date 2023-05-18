package com.redismq.samples.rocket;

import lombok.Data;

@Data
public class MessageCommitLog {
    /**
     * 消息长度
     */
    private int totalSize;


    /**
     * 主题
     */
    private String topic;

    /**
     * 标签
     */
    private String tag = "";

    /**
     * 虚拟队列名称 内部生成 外部设置无效
     */
    private String virtualQueueName;
}
