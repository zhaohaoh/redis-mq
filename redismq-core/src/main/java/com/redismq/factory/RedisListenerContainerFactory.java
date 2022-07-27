package com.redismq.factory;


import com.redismq.container.AbstractMessageListenerContainer;
import com.redismq.queue.Queue;

public interface RedisListenerContainerFactory {

    AbstractMessageListenerContainer createListenerContainer(Queue queue);
}
