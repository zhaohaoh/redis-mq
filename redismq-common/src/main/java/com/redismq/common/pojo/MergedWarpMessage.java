package com.redismq.common.pojo;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

/**
 * 合并消息
 *
 * @author hzh
 * @date 2024/05/05
 */
@Data
public class MergedWarpMessage {
    
    private List<Message> messages = new ArrayList<>();
    
    private PushMessage pushMessage;
}
