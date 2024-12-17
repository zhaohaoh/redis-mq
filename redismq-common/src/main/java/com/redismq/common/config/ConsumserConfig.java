package com.redismq.common.config;

import com.redismq.common.constant.OffsetEnum;
import lombok.Data;

/**
 * 消费者配置
 */
@Data
public class ConsumserConfig {
    
    /**
     * 应用名
     */
    private String applicationName = "redisMQ_client";
    /**
     * 消费者组id
     */
    private String groupId ="consumerGroup";
    
    /**
     * 多个消费者组偏移量最大允许偏差值，  超过偏差值的消息将会被移除
     */
    private Integer groupOffsetLowMax = 10000;
    
    /**
     * 该配置标识当前group重新上线时从哪个偏移量开始消费  latest最新偏移量    current当前偏移量。
     */
    private OffsetEnum autoOffsetConsume = OffsetEnum.CURRENT;
    
    /**
     * 该配置标识一个新的group上线时从哪个偏移量开始消费  latest最新偏移量  new从0开始消费。
     * 如果是current 新注册的group默认是0会从最开始的时候消费
     */
    private OffsetEnum newGroupOffset = OffsetEnum.LATEST;
}
