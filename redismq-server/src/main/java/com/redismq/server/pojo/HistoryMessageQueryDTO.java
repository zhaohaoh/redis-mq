package com.redismq.server.pojo;

import lombok.Data;

@Data
public class HistoryMessageQueryDTO {
    
    private String queueName;
    
    private String vQueue;
    
    private Long offset;
    
    private Long lastOffset;
    
    private Long page;
    
    private Long size;
}
