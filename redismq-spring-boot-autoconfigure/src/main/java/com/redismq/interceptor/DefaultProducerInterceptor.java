package com.redismq.interceptor;

import com.redismq.common.pojo.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;

public class DefaultProducerInterceptor implements ProducerInterceptor {
    Logger log = LoggerFactory.getLogger(ProducerInterceptor.class);


    @Override
    public void onFail(List<Message> messages, Exception e) {
        log.error("RedisMQ sendMessage Fail messages:{} Exception:", messages, e);
    }
}
