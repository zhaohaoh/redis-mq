package com.redismq.pojo;

import com.redismq.Message;
import lombok.Data;

import java.util.HashMap;
import java.util.Map;

@Data
public class SendMessageParamInner {
    Map<String,Long> messageExecutorTimeMap = new HashMap<>();
    Map<String, Message> messageMap = new HashMap<>();
}
