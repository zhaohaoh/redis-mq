package com.redismq.server.pojo;

import lombok.Data;

@Data
public class QueuePageSelect extends BasePageSelect{
    private boolean delayState;
    
}
