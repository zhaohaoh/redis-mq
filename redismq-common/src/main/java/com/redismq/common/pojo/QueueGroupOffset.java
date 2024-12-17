package com.redismq.common.pojo;

import lombok.Data;

@Data
public class QueueGroupOffset {
    private String vQueue;
    private Long offset;
    private Long lastOffset;
}
