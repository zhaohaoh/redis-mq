package com.redismq.common.constant;

import lombok.Getter;

@Getter
public enum MessageStatus {
    //消息创建状态
    CREATE(0),
    //消息成功状态
    SUCCESS(1),
    //消息失败状态
    FAIL(2);
    private final int code;
    
    MessageStatus(int code) {
        this.code = code;
    }
}
