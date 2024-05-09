package com.redismq.common.constant;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public enum ProducerAck {

    /**
     * 生产者确认机制
     */
    ASYNC("async" ),
    SYNC("sync" ),
    ALL("all");

    private final String name;
}
