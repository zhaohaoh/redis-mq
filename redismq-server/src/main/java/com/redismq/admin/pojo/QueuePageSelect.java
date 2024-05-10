package com.redismq.admin.pojo;

import lombok.Data;

@Data
public class QueuePageSelect extends BasePageSelect{
    private boolean delayState;
    
}
