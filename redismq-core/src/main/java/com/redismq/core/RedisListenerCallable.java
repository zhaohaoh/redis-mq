package com.redismq.core;

import com.redismq.Message;
import com.redismq.connection.RedisMQClientUtil;
import com.redismq.constant.AckMode;
import com.redismq.exception.RedisMqException;
import com.redismq.interceptor.ConsumeInterceptor;
import com.redismq.utils.RedisMQStringMapper;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * @Author: hzh
 * @Date: 2022/5/19 11:28
 * 执行器
 */
public class RedisListenerCallable implements Callable<Boolean> {
    protected static final Logger log = LoggerFactory.getLogger(RedisListenerCallable.class);

    /**
     *重试最大次数
     */
    private final int retryMax;
    /**
     *目标类
     */
    private final Object target;
    /**
     *参数
     */
    private Object args;
    /**
     *方法
     */
    private final Method method;
    /**
     *消费状态
     */
    private final PollState state = new PollState();
    /**
     *redis的客户端
     */
    private final RedisMQClientUtil redisMQClientUtil;
    /**
     * ack的模式
     */
    private String ackMode;
    /**
     *重试间隔 睡眠时间
     */
    private Integer retryInterval;
    /**
     *真实队列名
     */
    private String queueName;

    /**
     *最大重试次数
     */
    private int retryCount;


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



    public RedisMQClientUtil redisMQClientUtil() {
        return redisMQClientUtil;
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

    public RedisListenerCallable(Object target, Method method, int retryMax, RedisMQClientUtil redisMQClientUtil) {
        this.target = target;
        this.method = method;
        this.retryMax = retryMax;
        this.redisMQClientUtil = redisMQClientUtil;
    }

    @Override
    public Boolean call()   {
        state.starting();
        try {
            do {
                try {
                    run0();
                } catch (RedisMqException e) {
                    onFail((Message) args, e);
                    break;
                }
            } while (state.isActive());
        } finally {
            Message message = (Message) args;
            //如果是手动确认的话需要手动删除
            if (AckMode.MAUAL.equals(ackMode)) {
                redisMQClientUtil.removeMessage(message.getVirtualQueueName(), message);
            }
        }
        return true;
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
                //是内部message消息
                this.method.invoke(this.target, clone);
            } else {
                //监听类的参数不是Message
                Object body = clone.getBody();
                if (paramType.equals(body.getClass())) {
                    //对象相同直接处理
                    this.method.invoke(this.target, body);
                }else if (body.getClass().equals(String.class) ){
                    //对象是string，但是和监听类不是相同类
                    body= RedisMQStringMapper.toBean((String) body, paramType);
                    this.method.invoke(this.target, body);
                }else{
                    throw new RedisMqException("ClassNotConvert paramType: "+paramType +"messageClass: "+clone.getClass());
                }
            }
            state.finsh();
            log.debug("redisMQ consumeMessage success queue:{} tag:{}", message.getQueue(), message.getTag());
            afterConsume(clone);
        } catch (Exception e) {
            if (e instanceof InvocationTargetException){
                e = (Exception) e.getCause();
            }
            if (retryCount > retryMax +1) {
                state.cancel();
                log.error("redisMQ run retryMax:{} Cancel", retryMax);
                throw new RedisMqException("RedisListenerRunnable retryMax :", e);
            } else {
                log.error("RedisListenerRunnable consumeMessage run ERROR:", e);
                try {
                    Thread.sleep(retryInterval);
                } catch (InterruptedException interruptedException) {
                    log.error("redisMQ consumeMessage InterruptedException", interruptedException);
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

