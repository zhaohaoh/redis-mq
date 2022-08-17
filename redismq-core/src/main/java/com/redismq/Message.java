package com.redismq;

import com.redismq.core.RedisListenerRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Objects;
import java.util.UUID;

public class Message implements Serializable {
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

    private Object body;

    private String id = UUID.randomUUID().toString().replace("-", "");

    private String topic;
    //默认根据字符串匹配
    private String tag = "";

    private String virtualQueueName;

    public String getVirtualQueueName() {
        return virtualQueueName;
    }

    public void setVirtualQueueName(String virtualQueueName) {
        this.virtualQueueName = virtualQueueName;
    }

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }


    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Object getBody() {
        return body;
    }

    public void setBody(Object body) {
        this.body = body;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Message message = (Message) o;
        return Objects.equals(body, message.body) && Objects.equals(id, message.id) && Objects.equals(topic, message.topic) && Objects.equals(tag, message.tag) && Objects.equals(virtualQueueName, message.virtualQueueName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(body, id, topic, tag, virtualQueueName);
    }
}
