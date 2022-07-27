package com.redismq;

import java.io.*;
import java.util.Objects;
import java.util.UUID;

public class Message implements Serializable {
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

    private static final long serialVersionUID = 1L;

    private Object content;
    private String id = UUID.randomUUID().toString().replace("-", "");
    private Long tenantId;
    private Boolean restartState = false;
    //默认根据字符串匹配
    private String tag = "";

    private String queueName;

    public String getQueueName() {
        return queueName;
    }

    public void setQueueName(String queueName) {
        this.queueName = queueName;
    }

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    public Long getTenantId() {
        return tenantId;
    }

    public void setTenantId(Long tenantId) {
        this.tenantId = tenantId;
    }

    public Boolean getRestartState() {
        return restartState;
    }

    public void setRestartState(Boolean restartState) {
        this.restartState = restartState;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Object getContent() {
        return content;
    }

    public void setContent(Object content) {
        this.content = content;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Message message = (Message) o;
        return Objects.equals(content, message.content) && Objects.equals(id, message.id) && Objects.equals(tenantId, message.tenantId) && Objects.equals(restartState, message.restartState) && Objects.equals(tag, message.tag) && Objects.equals(queueName, message.queueName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(content, id, tenantId, restartState, tag, queueName);
    }
}
