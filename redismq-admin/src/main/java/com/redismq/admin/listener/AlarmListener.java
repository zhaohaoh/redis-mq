//package com.redismq.admin.listener;
//
//import com.redismq.common.pojo.PushMessage;
//import com.redismq.common.constant.RedisMQConstant;
//import org.redisson.api.RTopic;
//import org.redisson.api.RedissonClient;
//import org.redisson.codec.SerializationCodec;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import org.springframework.stereotype.Component;
//
//import javax.annotation.PostConstruct;
//import javax.annotation.Resource;
//
//@Component
//public class AlarmListener {
//
//
//    private static final Logger LOGGER = LoggerFactory.getLogger(AlarmListener.class);
//
//    @Resource
//    private RedissonClient redisson;
//    public static String WS_ALARM_LISTEN = RedisMQConstant.getTopic();
//    private RTopic topic;
//
//
//
//
//    /**
//     * 开启监听
//     */
//    @PostConstruct
//    void openReceiving() {
//        topic = redisson.getTopic(WS_ALARM_LISTEN, new SerializationCodec());
//        LOGGER.info("监听ws成功：{}", topic);
//
//        topic.addListener(PushMessage.class, (charSequence, msgStr) -> {
//            System.out.println(msgStr);
//        });
//    }
//}