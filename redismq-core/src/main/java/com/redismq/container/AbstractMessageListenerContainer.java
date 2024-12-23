package com.redismq.container;

import com.redismq.common.connection.RedisMQClientUtil;
import com.redismq.common.pojo.Queue;
import com.redismq.core.RedisListenerCallable;
import com.redismq.core.RedisListenerEndpoint;
import com.redismq.delay.DelayTimeoutTaskManager;
import com.redismq.interceptor.ConsumeInterceptor;
import lombok.Data;

import java.util.List;
import java.util.Map;

import static com.redismq.common.constant.GlobalConstant.SPLITE;
import static com.redismq.common.constant.StateConstant.PAUSE;
import static com.redismq.common.constant.StateConstant.RUNNING;
import static com.redismq.common.constant.StateConstant.STOP;

/**
 * 容器抽象类
 */
@Data
public abstract class AbstractMessageListenerContainer {

    /**
     * 队列名
     */
    protected String queueName;

    /**
     * redis
     */
    protected RedisMQClientUtil redisMQClientUtil;
    /**
     * 最小线程数
     */
    protected int concurrency;
    /**
     * 最大线程数量
     */
    protected int maxConcurrency;
    /**
     * 重试最大次数
     */
    protected int retryMax;
    /**
     * 延迟队列
     */
    protected Boolean delay;
    /**
     * ack模式
     */
    protected String ackMode;
    /**
     * 重试间隔
     */
    protected Integer retryInterval;
    /**
     * 任务执行超时时间
     */
    protected long taskTimeout;
    /**
     * 任务阻塞等待轮询时间
     */
    protected long taskWaitTime;
    /**
     * 端点map
     */
    protected Map<String, RedisListenerEndpoint> redisListenerEndpointMap;
    /**
     * 消费拦截器
     */
    protected List<ConsumeInterceptor> consumeInterceptorList;
    /**
     * 状态描述
     */
    protected volatile int state = RUNNING;
    

    
    /**
     * 延时任务管理器
     */
    protected final DelayTimeoutTaskManager delayTimeoutTaskManager = new DelayTimeoutTaskManager();

    public void setConsumeInterceptorList(List<ConsumeInterceptor> consumeInterceptorList) {
        this.consumeInterceptorList = consumeInterceptorList;
    }

    public void setRedisListenerEndpointMap(Map<String, RedisListenerEndpoint> redisListenerEndpointMap) {
        this.redisListenerEndpointMap = redisListenerEndpointMap;
    }

    public AbstractMessageListenerContainer(RedisMQClientUtil redisMQClientUtil, Queue queue,List<ConsumeInterceptor> consumeInterceptorList
     ) {
        this.queueName = queue.getQueueName();
        this.redisMQClientUtil = redisMQClientUtil;
        this.concurrency = queue.getConcurrency();
        this.maxConcurrency = queue.getMaxConcurrency();
        this.delay = queue.isDelayState();
        this.retryMax = queue.getRetryMax();
        this.ackMode = queue.getAckMode();
        this.retryInterval = queue.getRetryInterval();
        this.consumeInterceptorList =  consumeInterceptorList;
    }

    /**
     * 当前容器暂停拉取消息
     */
    public int pause() {
        if (state!=PAUSE){
            state = PAUSE;
        }
        delayTimeoutTaskManager.stop();
        return PAUSE;
    }
    /**
     * 当前容器是否暂停了
     */
    public boolean isPause() {
        return state == PAUSE;
    }

    public abstract void repush();
    /**
     * 当前容器是否停止了
     */
    public void stop() {
        this.state = STOP;
        doStop();
    }

    public abstract void doStop();

    public boolean isRunning() {
        return state == RUNNING;
    }

    public void running() {
        if (state != RUNNING) {
            state = RUNNING;
        }
    }

    protected RedisListenerCallable getRedisListenerCallable(String id, Object message) {
        RedisListenerEndpoint redisListenerEndpoint = redisListenerEndpointMap.get(id);
        if (redisListenerEndpoint == null) {
           return null;
        }
        RedisListenerCallable runnable = new RedisListenerCallable(redisListenerEndpoint.getBean(), redisListenerEndpoint.getMethod(),
                this.getRetryMax(),
                this.redisMQClientUtil);
        runnable.setArgs(message);
        runnable.setAckMode(this.getAckMode());
        runnable.setRetryInterval(this.getRetryInterval());
        runnable.setQueueName(queueName);
        runnable.setConsumeInterceptors(consumeInterceptorList);
        return runnable;
    }

    protected String getRunableKey(String tag) {
        return queueName + SPLITE + tag;
    }
    
    //获取锁和释放锁的动作只能有一个线程执行 后面要优化成redisson
    public void unLockQueue(String virtualQueueLock) {
        redisMQClientUtil.unlock(virtualQueueLock);
    }
    
}