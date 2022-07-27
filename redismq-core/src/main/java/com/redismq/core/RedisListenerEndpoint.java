package com.redismq.core;


import java.lang.reflect.Method;
/**
 * @Author: hzh
 * @Date: 2022/5/19 11:27
 * 监听的方法参数断点封装
 */
public class RedisListenerEndpoint {
    private Object bean;
    private Method method;
    private String id;
    private String tag;

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    public Object getBean() {
        return bean;
    }

    public void setBean(Object bean) {
        this.bean = bean;
    }

    public Method getMethod() {
        return method;
    }

    public void setMethod(Method method) {
        this.method = method;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

}
