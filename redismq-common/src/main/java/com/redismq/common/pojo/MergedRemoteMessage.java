package com.redismq.common.pojo;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
public class MergedRemoteMessage {
    
    private List<RemoteMessage> messages = new ArrayList<>();
    
    
}
