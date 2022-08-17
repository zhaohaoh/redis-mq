package com.redismq.core;

import com.redismq.Message;
import com.redismq.constant.AckMode;
import com.redismq.exception.RedisMqException;
import com.redismq.interceptor.ConsumeInterceptor;
import com.redismq.queue.QueueManager;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @Author: hzh
 * @Date: 2022/5/19 11:28
 * 执行器
 */
public class RedisListenerRunnable implements Runnable {
    protected static final Logger log = LoggerFactory.getLogger(RedisListenerRunnable.class);
    //重试最大次数
    private final int retryMax;
    private final Object target;
    private Object args;
    private final Method method;
    private final PollState state = new PollState();
    private final RedisTemplate<String, Object> redisTemplate;
    private Semaphore semaphore;
    private String ackMode;
    private Integer retryInterval;
    //真实队列名
    private String queueName;
    //本地消息表
    private Set<Message> localMessages;
    private List<ConsumeInterceptor> consumeInterceptors;

    public List<ConsumeInterceptor> getConsumeInterceptors() {
        return consumeInterceptors;
    }

    public void setConsumeInterceptors(List<ConsumeInterceptor> consumeInterceptors) {
        this.consumeInterceptors = consumeInterceptors;
    }

    public void setLocalMessages(Set<Message> localMessages) {
        this.localMessages = localMessages;
    }

    public String getQueueName() {
        return queueName;
    }

    public void setQueueName(String queueName) {
        this.queueName = queueName;
    }

    public void setDelayState(boolean delayState) {
    }

    public Integer getRetryInterval() {
        return retryInterval;
    }

    public void setRetryInterval(Integer retryInterval) {
        this.retryInterval = retryInterval;
    }

    public String getAckMode() {
        return ackMode;
    }

    public void setAckMode(String ackMode) {
        this.ackMode = ackMode;
    }

    public Object getArgs() {
        return args;
    }

    public void setArgs(Object args) {
        this.args = args;
    }


    public Semaphore getSemaphore() {
        return semaphore;
    }

    public void setSemaphore(Semaphore semaphore) {
        this.semaphore = semaphore;
    }

    public RedisTemplate<String, Object> getRedisTemplate() {
        return redisTemplate;
    }

    /**
     * Return the target instance to call the method on.
     */
    public Object getTarget() {
        return this.target;
    }

    /**
     * Return the target method to call.
     */
    public Method getMethod() {
        return this.method;
    }

    public RedisListenerRunnable(Object target, Method method, int retryMax, Semaphore semaphore, RedisTemplate<String, Object> redisTemplate) {
        this.target = target;
        this.method = method;
        this.retryMax = retryMax;
        this.semaphore = semaphore;
        this.redisTemplate = redisTemplate;
    }

    @Override
    public void run() {
        state.starting();
        try {
            AtomicInteger atomicInteger = new AtomicInteger(0);
            do {
                run0(atomicInteger);
            } while (state.isActive());
            Message message = (Message) args;
            //如果是手动确认的话需要手动删除
            if (state.isFinsh() && AckMode.MAUAL.equals(ackMode)) {
                String queueName = QueueManager.getQueueNameByVirtual(message.getVirtualQueueName());
                Long count = redisTemplate.opsForZSet().remove(queueName, args);
                localMessages.remove(message);
            }
        } finally {
            if (semaphore != null) {
                semaphore.release();
            }
        }
    }

    private void run0(AtomicInteger atomicInteger) {
        int i = atomicInteger.incrementAndGet();
        try {
            Class<?>[] parameterTypes = method.getParameterTypes();
            if (ArrayUtils.isEmpty(parameterTypes)) {
                throw new RedisMqException("redismq consume no has message");
            }
            Class<?> paramType = parameterTypes[0];
            state.running();
            ReflectionUtils.makeAccessible(this.method);
            Message message = (Message) args;
            // 拷贝对象.原对象不会发生改变.否则对象改变了无法删除redis中的数据
            Message clone = message.deepClone();
            clone = beforeConsume(clone);
            //参数是Message或者是实体类都可以
            if (paramType.equals(clone.getClass())) {
                this.method.invoke(this.target, clone);
            } else if (paramType.equals(clone.getBody().getClass())) {
                this.method.invoke(this.target, clone.getBody());
            }
            state.finsh();
            log.debug("redismq消息消费成功 tag:{}", message.getTag());
            afterConsume(clone);
        } catch (Exception e) {
            log.error("redismq 执行失败", e);
            if (AckMode.MAUAL.equals(ackMode) && i > retryMax) {
                state.cancel();
                log.error("redismq 手动ack执行失败超过指定次数:{} 任务取消", retryMax);
            } else if (AckMode.AUTO.equals(ackMode)) {
                state.cancel();
            } else {
                try {
                    Thread.sleep(retryInterval);
                } catch (InterruptedException interruptedException) {
                    log.error("redismq 执行失败睡眠重试InterruptedException", interruptedException);
                    Thread.currentThread().interrupt();
                }
            }
            onFail((Message) args, e);
        }
    }

    private void onFail(Message args, Exception e) {
        if (!CollectionUtils.isEmpty(consumeInterceptors)) {
            for (ConsumeInterceptor consumeInterceptor : consumeInterceptors) {
                consumeInterceptor.onFail(args, e);
            }
        }
    }

    private Message beforeConsume(Message args) {
        if (!CollectionUtils.isEmpty(consumeInterceptors)) {
            for (ConsumeInterceptor consumeInterceptor : consumeInterceptors) {
                args = consumeInterceptor.beforeConsume(args);
            }
        }
        return args;
    }

    private void afterConsume(Message args) {
        if (!CollectionUtils.isEmpty(consumeInterceptors)) {
            for (ConsumeInterceptor consumeInterceptor : consumeInterceptors) {
                consumeInterceptor.afterConsume(args);
            }
        }
    }


    @Override
    public String toString() {
        return this.method.getDeclaringClass().getName() + "." + this.method.getName();
    }


    public static class PollState {
        enum State {
            CREATED, STARTING, RUNNING, CANCELLED, FINSH;
        }

        private volatile State state = State.CREATED;

        public State getState() {
            return state;
        }

        boolean isActive() {
            return state == State.STARTING || state == State.RUNNING;
        }

        boolean isFinsh() {
            return state == State.FINSH;
        }

        void starting() {
            state = State.STARTING;
        }


        void running() {
            state = State.RUNNING;
        }


        void cancel() {
            state = State.CANCELLED;
        }

        void finsh() {
            state = State.FINSH;
        }
    }
}

