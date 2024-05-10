package com.redismq.common.constant;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public enum ProducerAck {

    /**
     * 异步确认
     */
    ASYNC("async" ),
    /**
     * 同步确认
     */
    SYNC("sync" );
    /**
     * 暂无，不可选。预留
     */
//    ALL("all");

    private final String name;
}
