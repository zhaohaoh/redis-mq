package com.redismq.core;

import com.redismq.Message;
import com.redismq.connection.RedisClient;
import com.redismq.constant.AckMode;
import com.redismq.exception.RedisMqException;
import com.redismq.interceptor.ConsumeInterceptor;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;

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
    private final RedisClient redisClient;
    private Semaphore semaphore;
    private String ackMode;
    private Integer retryInterval;
    //真实队列名
    private String queueName;
    private Map<String, Message> localMessages;
    private int retryCount;

    public void setLocalMessages(Map<String, Message> localMessages) {
        this.localMessages = localMessages;
    }

    public String getQueueName() {
        return queueName;
    }

    private List<ConsumeInterceptor> consumeInterceptors;

    public void setConsumeInterceptors(List<ConsumeInterceptor> consumeInterceptors) {
        this.consumeInterceptors = consumeInterceptors;
    }

    public void setQueueName(String queueName) {
        this.queueName = queueName;
    }

    public void setRetryInterval(Integer retryInterval) {
        this.retryInterval = retryInterval;
    }

    public void setAckMode(String ackMode) {
        this.ackMode = ackMode;
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

    public RedisClient getRedisClient() {
        return redisClient;
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

    public RedisListenerRunnable(Object target, Method method, int retryMax, Semaphore semaphore, RedisClient redisClient) {
        this.target = target;
        this.method = method;
        this.retryMax = retryMax;
        this.semaphore = semaphore;
        this.redisClient = redisClient;
    }

    @Override
    public void run() {
        state.starting();
        try {
            do {
                run0();
            } while (state.isActive());
        } catch (Exception e) {
            onFail((Message) args, e);
        } finally {
            Message message = (Message) args;
            semaphore.release();
            //如果是手动确认的话需要手动删除
            if (state.isFinsh() && AckMode.MAUAL.equals(ackMode)) {
                Long count = redisClient.zRemove(message.getVirtualQueueName(), args);
            }
            localMessages.remove(message.getId());
        }
    }

    private void run0() {
        retryCount++;
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
            log.debug("redismq consumeMessage success topic:{} tag:{}", message.getTopic(), message.getTag());
            afterConsume(clone);
        } catch (Exception e) {
            log.error("RedisListenerRunnable consumeMessage run ERROR:", e);
            if (retryCount > retryMax) {
                state.cancel();
                log.error("redismq run0 retryMax:{} Cancel", retryMax);
                throw new RedisMqException("RedisListenerRunnable retryMax run0:", e);
            } else {
                try {
                    Thread.sleep(retryInterval);
                } catch (InterruptedException interruptedException) {
                    log.error("redismq consumeMessage InterruptedException", interruptedException);
                    Thread.currentThread().interrupt();
                }
            }
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
                consumeInterceptor.beforeConsume(args);
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

