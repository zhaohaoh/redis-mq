package com.redismq;


import com.redismq.common.config.GlobalConfigCache;
import com.redismq.common.connection.RedisMQClientUtil;
import com.redismq.common.constant.OffsetEnum;
import com.redismq.common.exception.RedisMqException;
import com.redismq.common.pojo.Queue;
import com.redismq.common.serializer.RedisMQStringMapper;
import com.redismq.container.AbstractMessageListenerContainer;
import com.redismq.container.RedisMQListenerContainer;
import com.redismq.core.RedisListenerContainerManager;
import com.redismq.core.RedisListenerEndpoint;
import com.redismq.core.RedisMqClient;
import com.redismq.interceptor.ConsumeInterceptor;
import com.redismq.queue.QueueManager;
import com.redismq.rpc.client.RemotingClient;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.aop.framework.AopProxyUtils;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.SmartLifecycle;
import org.springframework.core.MethodIntrospector;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.data.redis.listener.adapter.MessageListenerAdapter;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.util.CollectionUtils;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static com.redismq.common.constant.GlobalConstant.SPLITE;
import static com.redismq.common.constant.RedisMQConstant.getOffsetGroupCollection;


//Bean的后置处理器切入点
public class RedisMqAnnotationBeanPostProcessor implements BeanPostProcessor, Ordered, ApplicationContextAware, SmartLifecycle, DisposableBean {
    protected final Log logger = LogFactory.getLog(getClass());
    private volatile boolean isRunning = false;
    private ApplicationContext applicationContext;
    private final Set<Class<?>> nonAnnotatedClasses = Collections.newSetFromMap(new ConcurrentHashMap<>(64));
    private final Map<String, List<RedisListenerEndpoint>> redisListenerEndpointMap = new ConcurrentHashMap<>();
    //    为了让RedisMQAutoConfiguration加载执行init方法
//    private RedisMqClient redisMqClient;


    //bean初始化后的回调方法 查找出RedisListener注解标记的类
    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        //获取代理类的最终类
        Class<?> targetClass = AopProxyUtils.ultimateTargetClass(bean);
        if (!this.nonAnnotatedClasses.contains(targetClass) &&
                AnnotationUtils.isCandidateClass(targetClass, Collections.singletonList(RedisListener.class))) {
            //从当前类中寻找RedisListe ner注解。一个方法可能多有多个 所以用set
            Map<Method, RedisListener> annotatedMethods = MethodIntrospector.selectMethods(targetClass,
                    (MethodIntrospector.MetadataLookup<RedisListener>) method -> {
                        RedisListener redisListenerMethods = AnnotatedElementUtils.findMergedAnnotation(
                                method, RedisListener.class);
                        return (!(redisListenerMethods == null) ? redisListenerMethods : null);
                    });
            if (annotatedMethods.isEmpty()) {
                this.nonAnnotatedClasses.add(targetClass);
                if (logger.isTraceEnabled()) {
                    logger.trace("No @RedisListener annotations found on bean class: " + targetClass);
                }
            } else {
                //校验重复队列名的注解
                Collection<RedisListener> values = annotatedMethods.values();
                Map<String, List<RedisListener>> listMap = values.stream().collect(Collectors.groupingBy(RedisListener::queue));
                listMap.forEach((k, v) -> {
                    if (StringUtils.isBlank(k)) {
                        throw new RedisMqException("redismq queue name not null");
                    }
                    List<String[]> list = v.stream().map(RedisListener::tag).collect(Collectors.toList());
                    List<String> nameList = new ArrayList<>();
                    for (String[] tags : list) {
                        nameList.addAll(Arrays.stream(tags).collect(Collectors.toList()));
                    }
                    if (nameList.stream().distinct().count() != nameList.size()) {
                        throw new RedisMqException("redismq  duplicate queueName");
                    }
                });

                // Non-empty set of methods
                annotatedMethods.forEach((method, redisListener) -> process(redisListener, method, bean));

                if (logger.isTraceEnabled()) {
                    logger.trace(annotatedMethods.size() + " @RedisListener methods processed on bean '" + beanName +
                            "': " + annotatedMethods);
                }
            }
        }
        return bean;
    }

    //处理RedisListener注解
    private void process(RedisListener redisListener, Method method, Object bean) {
        RedisMqClient redisMqClient = applicationContext.getBean(RedisMqClient.class);
        //添加订阅监听类  为了快速使用spring封装好的监听容器.  发布订阅的实现
        if (StringUtils.isNotBlank(redisListener.channelTopic())) {
            handlerPubSub(redisListener, method, bean);
            return;
        }
        Queue queue = new Queue(redisListener.queue());
        //初始化队列设置默认值
        initQueue(queue);

        //注解中有配置以注解的配置优先
        if (redisListener.concurrency() > 0) {
            queue.setConcurrency(redisListener.concurrency());
        }
        if (redisListener.maxConcurrency() > 0) {
            queue.setMaxConcurrency(redisListener.maxConcurrency());
        }
        if (redisListener.retryMax() >= 0) {
            queue.setRetryMax(redisListener.retryMax());
        }
        if (redisListener.retryInterval() > 0) {
            queue.setRetryInterval(redisListener.retryInterval());
        }
        if (StringUtils.isNotBlank(redisListener.ackMode())) {
            queue.setAckMode(redisListener.ackMode());
        }
        queue.setDelayState(redisListener.delay());
        if (redisListener.virtual() > 0) {
            queue.setVirtual(redisListener.virtual());
        }
        if (redisListener.queueMaxSize() > 0) {
            queue.setQueueMaxSize(redisListener.queueMaxSize());
        }
    
    
        redisMqClient.registerQueue(queue);

        QueueManager.registerLocalQueue(queue);

        //反射获取方法
        Method invocableMethod = AopUtils.selectInvocableMethod(method, bean.getClass());

        //监听端点 封装方法名 bean名字 和routingKey一对一。一个队列可能有多个
        List<RedisListenerEndpoint> redisListenerEndpoints = redisListenerEndpointMap.computeIfAbsent(queue.getQueueName(), q -> new ArrayList<>());
        for (String tag : redisListener.tag()) {
            RedisListenerEndpoint redisListenerEndpoint = new RedisListenerEndpoint();
            redisListenerEndpoint.setTag(tag);
            redisListenerEndpoint.setBean(bean);
            redisListenerEndpoint.setMethod(invocableMethod);
            redisListenerEndpoint.setId(queue.getQueueName() + SPLITE + tag);
            redisListenerEndpoints.add(redisListenerEndpoint);
        }
    }

    
    @Override
    public int getOrder() {
        return LOWEST_PRECEDENCE;
    }


    @Override
    public void start() {
        RedisMqClient redisMqClient = applicationContext.getBean(RedisMqClient.class);
        this.registryBeanQueue();
        redisMqClient.getAllQueue().forEach(QueueManager::registerRedisQueue);
        if (!CollectionUtils.isEmpty(QueueManager.getLocalQueues())) {
            isRunning = this.createContainer();
            //如果没有创建容器说明是生产者，生产者不启动监听配置
            if (isRunning) {
                redisMqClient.start();
            }
        }
    }

    /**
     * 创建容器
     */
    private boolean createContainer() {
        Map<String, Queue> queues = QueueManager.getLocalQueueMap();
        //设置工厂中的属性，工厂生成的属性和最终队列属性一致
    
        RedisMQClientUtil redisMQClientUtil = applicationContext.getBean("redisMQClientUtil", RedisMQClientUtil.class);
        Map<String, ConsumeInterceptor> consumeInterceptorMap = applicationContext.getBeansOfType(ConsumeInterceptor.class);
        //没有配置取全局配置
        AtomicBoolean create = new AtomicBoolean(false);
        RedisMqClient redisMqClient = applicationContext.getBean(RedisMqClient.class);
        queues.forEach((name, queue) -> {
            List<RedisListenerEndpoint> listenerEndpoints = redisListenerEndpointMap.get(name);
            if (CollectionUtils.isEmpty(listenerEndpoints)) {
                return;
            }
            create.set(true);
            ObjectProvider<RemotingClient> beanProvider = applicationContext.getBeanProvider(RemotingClient.class);
            
            RemotingClient remotingClient = beanProvider.getIfAvailable();
            
            String groupId = GlobalConfigCache.CONSUMER_CONFIG.getGroupId();
            //消费者组偏移量
            Long queueGroupOffset = redisMQClientUtil.getQueueGroupOffset(getOffsetGroupCollection(groupId),
                    queue.getQueueName());
            //队列偏移量
            Long queueMaxOffset = redisMQClientUtil.getQueueMaxOffset(queue.getQueueName());
            
            // 第一次上线的消费者组，从哪里开始消费
            OffsetEnum newGroupOffset = GlobalConfigCache.CONSUMER_CONFIG.getNewGroupOffset();
            if (newGroupOffset.equals(OffsetEnum.LATEST) && queueGroupOffset==0L){
                queueGroupOffset = queueMaxOffset;
            }
            
            // 中断了很久，重新上线的消费者组从哪里开始消费
            OffsetEnum autoOffsetConsume = GlobalConfigCache.CONSUMER_CONFIG.getAutoOffsetConsume();
            if (autoOffsetConsume.equals(OffsetEnum.LATEST)){
                queueGroupOffset = queueMaxOffset;
            }
            
            AbstractMessageListenerContainer listenerContainer = new RedisMQListenerContainer(redisMQClientUtil, queue,
                    CollectionUtils.isEmpty(consumeInterceptorMap) ?
                    new ArrayList<>() : new ArrayList<>(consumeInterceptorMap.values())
                    ,remotingClient
            , queueGroupOffset,queueMaxOffset);
            
            RedisListenerContainerManager redisListenerContainerManager = redisMqClient.getRedisListenerContainerManager();
            redisListenerContainerManager.registerContainer(listenerContainer, listenerEndpoints);
        });
        return create.get();
    }

    //继承SmartLifecycle   容器停止执行调用
    @Override
    public void stop(Runnable callback) {
        stop();
        callback.run();
    }

    @Override
    public int getPhase() {
        return 0;
    }

    @Override
    public boolean isAutoStartup() {
        return true;
    }

    @Override
    public void stop() {
        if (!CollectionUtils.isEmpty(QueueManager.getLocalQueues())) {
            RedisMqClient redisMqClient = applicationContext.getBean(RedisMqClient.class);
            redisMqClient.destory();
            isRunning = false;
        }
    }

    @Override
    public boolean isRunning() {
        return isRunning;
    }

    @Override
    public void destroy() {
        stop();
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    private void registryBeanQueue() {
        Map<String, Queue> queueMap = applicationContext.getBeansOfType(Queue.class);
        if (!CollectionUtils.isEmpty(queueMap)) {
            Collection<Queue> queues = queueMap.values();
            for (Queue queue : queues) {
                if (StringUtils.isBlank(queue.getQueueName())) {
                    throw new RedisMqException("redismq  queueName not blank");
                }
            }
            List<String> nameList = queues.stream().map(Queue::getQueueName).collect(Collectors.toList());
            if (nameList.stream().distinct().count() != nameList.size()) {
                throw new RedisMqException("redismq duplicate queueName");
            }
            RedisMqClient redisMqClient = applicationContext.getBean(RedisMqClient.class);
            for (Queue queue : queues) {
                redisMqClient.registerQueue(queue);
            }
        }
    }

    private void handlerPubSub(RedisListener redisListener, Method method, Object bean) {
        RedisMessageListenerContainer container = applicationContext.getBean("redisMQMessageListenerContainer", RedisMessageListenerContainer.class);
        MessageListenerAdapter listener = new MessageListenerAdapter(bean, method.getName());
        //使用Jackson2JsonRedisSerializer来序列化和反序列化redis的value值（默认使用JDK的序列化方式）这种序列化速度中上，明文存储
        Jackson2JsonRedisSerializer<Object> jacksonSeial = new Jackson2JsonRedisSerializer<>(Object.class);
        jacksonSeial.setObjectMapper(RedisMQStringMapper.STRING_MAPPER);
        listener.setSerializer(jacksonSeial);
        listener.afterPropertiesSet();
        //名称完全对应的topic
        container.addMessageListener(listener, new ChannelTopic(redisListener.channelTopic()));
    }
    
    private void initQueue(Queue queue) {
        
        if (queue.getConcurrency() == null) {
            queue.setConcurrency(GlobalConfigCache.QUEUE_CONFIG.getConcurrency());
        }
        if (queue.getMaxConcurrency() == null) {
            queue.setMaxConcurrency(GlobalConfigCache.QUEUE_CONFIG.getMaxConcurrency());
        }
        if (queue.getAckMode() == null) {
            queue.setAckMode(GlobalConfigCache.QUEUE_CONFIG.getAckMode());
        }
        if (queue.getRetryMax() == null) {
            queue.setRetryMax(GlobalConfigCache.QUEUE_CONFIG.getConsumeRetryMax());
        }
        if (queue.getConcurrency() > queue.getMaxConcurrency()) {
            throw new RedisMqException("MaxConcurrency cannot be less than Concurrency");
        }
        if (queue.getRetryMax() > 10) {
            throw new RedisMqException("ConsumeRetryMax cannot be greater than 10");
        }
        if (queue.getRetryInterval() == null) {
            queue.setRetryInterval(GlobalConfigCache.QUEUE_CONFIG.getRetryInterval());
        }
        if (queue.getVirtual() == null) {
            queue.setVirtual(GlobalConfigCache.QUEUE_CONFIG.getVirtual());
        }
        //设置队列默认大小
        if (queue.getQueueMaxSize() == null || queue.getQueueMaxSize() <= 0) {
            queue.setQueueMaxSize(GlobalConfigCache.GLOBAL_CONFIG.getQueueMaxSize());
        }
    }
}
