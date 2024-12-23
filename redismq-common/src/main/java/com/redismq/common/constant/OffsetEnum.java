package com.redismq.common.constant;

public enum OffsetEnum {
    // 从最新偏移量开始消费
    LATEST,
    // 从当前偏移量开始消费
    CURRENT;
}
