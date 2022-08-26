package com.redismq.pojo;

import com.redismq.Message;

import java.util.Objects;

public class SendMessageParam {
    private Message message;
    private Long executorTime;

    public Message getMessage() {
        return message;
    }

    public void setMessage(Message message) {
        this.message = message;
    }

    public Long getExecutorTime() {
        return executorTime;
    }

    public void setExecutorTime(Long executorTime) {
        this.executorTime = executorTime;
    }

    @Override
    public String toString() {
        return "SendMessageParam{" +
                "messages=" + message +
                ", executorTime=" + executorTime +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SendMessageParam that = (SendMessageParam) o;
        return Objects.equals(message, that.message) && Objects.equals(executorTime, that.executorTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(message, executorTime);
    }
}
