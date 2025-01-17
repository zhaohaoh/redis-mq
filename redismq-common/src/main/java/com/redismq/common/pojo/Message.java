package com.redismq.common.pojo;

import com.redismq.common.serializer.RedisMQStringMapper;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.springframework.beans.BeanUtils;

import java.io.Serializable;
import java.util.Map;

/**
 * @Author: hzh
 * @Date: 2022/9/6 10:50
 * 消息体
 */
@Data
@AllArgsConstructor
public class Message implements Serializable {
    public Message() {
    }
    // Person 的构造函数私有化
    private Message(Builder builder) {
        this.body = builder.body;
        this.id = builder.id;
        this.key = builder.key;
        this.queue = builder.queue;
        this.tag = builder.tag;
        this.virtualQueueName = builder.virtualQueueName;
        this.header = builder.header;
    }
    
    private static final long serialVersionUID = 1L;

    public Message deepClone() {
        Message outer = new Message();
        try { // 将该对象序列化成流,因为写在流里的是对象的一个拷贝，而原对象仍然存在于JVM里面。所以利用这个特性可以实现对象的深拷贝
            BeanUtils.copyProperties(this,outer);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return outer;
    }
    public <T> T parseJavaBean(Class<T> tClass){
        if (body.getClass().equals(tClass)){
            return (T) body;
        }
        return RedisMQStringMapper.toBean((String)body,tClass);
    }

    /**
     * 消息主体  消息都会转为字符串存储
     */
    private Object body;

    /**
     * 消息id
     */
    private String id;
    
    
    /**
     * 用来路由虚拟队列的key
     */
    private String key="";

    /**
     * 队列
     */
    private String queue;

    /**
     * 标签
     */
    private String tag = "";

    /**
     * 虚拟队列名称
     */
    private String virtualQueueName;
    
    /**
     * 消息偏移量
     */
    private Long offset;
    
    
    /**
     * 执行时间redis分数
     */
    private Long executeScope;
    /**
     * 执行时间
     */
    private Long executeTime;
    
    /**
     * 消息头部
     */
    private Map<String,Object> header;
    
    public static Builder builder(){
        return new Builder();
    }
    // 建造器类
    public static class Builder {
        private Object body;
    
        /**
         * 消息id
         */
        private String id;
    
    
        /**
         * 用来路由虚拟队列的key
         */
        private String key="";
    
        /**
         * 队列
         */
        private String queue;
    
        /**
         * 标签
         */
        private String tag = "";
    
        /**
         * 虚拟队列名称 内部生成 外部设置无效
         */
        private String virtualQueueName;
        
        
        /**
         * 消息头部
         */
        private Map<String,Object> header;
    
    
        public Builder() {
        }
        
        public Builder body(Object body) {
            this.body = body;
            return this;
        }
        public Builder header(Map<String,Object> header) {
            this.header = header;
            return this;
        }
    
        public Builder id(String id) {
            this.id = id;
            return this;
        }
        public Builder key(String key) {
            this.key = key;
            return this;
        }
        public Builder queue(String queue) {
            this.queue = queue;
            return this;
        }
        public Builder tag(String tag) {
            this.tag = tag;
            return this;
        }
        public Builder virtualQueueName(String virtualQueueName) {
            this.virtualQueueName = virtualQueueName;
            return this;
        }
        
        // 构造 Person 对象并返回
        public Message build() {
            return new Message(this);
        }
    }
}
