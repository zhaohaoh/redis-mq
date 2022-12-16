package com.redismq;

import com.aventrix.jnanoid.jnanoid.NanoIdUtils;
import lombok.Builder;
import lombok.Data;

import java.io.*;
/**
 * @Author: hzh
 * @Date: 2022/9/6 10:50
 * 消息体
 */
@Data
@Builder
public class Message implements Serializable {
    public Message() {
    }

    public Message(Object body, String id, String topic, String tag, String virtualQueueName) {
        this.body = body;
        this.id = id;
        this.topic = topic;
        this.tag = tag;
        this.virtualQueueName = virtualQueueName;
    }

    private static final long serialVersionUID = 1L;

    public Message deepClone() {
        Message outer = null;
        try { // 将该对象序列化成流,因为写在流里的是对象的一个拷贝，而原对象仍然存在于JVM里面。所以利用这个特性可以实现对象的深拷贝
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(this);
            // 将流序列化成对象
            ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
            ObjectInputStream ois = new ObjectInputStream(bais);
            outer = (Message) ois.readObject();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return outer;
    }

    /**
     * 消息主体
     */
    private Object body;

    /**
     * 消息id
     */
    private String id = NanoIdUtils.randomNanoId();

    /**
     * 主题
     */
    private String topic;

    /**
     * 标签
     */
    private String tag = "";

    /**
     * 虚拟队列名称 内部生成 外部设置无效
     */
    private String virtualQueueName;
}
