package com.redismq.samples.consumer;

import com.redismq.common.serializer.RedisMQStringMapper;
import lombok.Data;

@Data
public class JavaBean  {
    private String a;
    private Integer b;

    private String ggg;
    private Integer ddd;
    
    private Long consumserTime;
    
    public static void main(String[] args) {
        JavaBean javaBean = RedisMQStringMapper
                .toBean("{ \"a\": \"valueA\", \"b\": 123, \"ggg\": \"valueGgg\", \"ddd\": 456 }", JavaBean.class);
        System.out.println(javaBean);
//        JavaBean javaBean1 = new JavaBean();
//        javaBean1.setB(1);
//        RedisMQObjectMapper.toJsonStr()
    }
}

