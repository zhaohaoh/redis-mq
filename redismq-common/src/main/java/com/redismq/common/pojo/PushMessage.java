package com.redismq.common.pojo;

import java.util.Objects;

public class PushMessage {
    //虚拟队列
    private String queue;
    private Long timestamp;

    public String getQueue() {
        return queue;
    }

    public void setQueue(String queue) {
        this.queue = queue;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "PushMessage{" +
                "queue='" + queue + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PushMessage that = (PushMessage) o;
        return Objects.equals(queue, that.queue) && Objects.equals(timestamp, that.timestamp);
    }

    @Override
    public int hashCode() {
        return Objects.hash(queue, timestamp);
    }
}
