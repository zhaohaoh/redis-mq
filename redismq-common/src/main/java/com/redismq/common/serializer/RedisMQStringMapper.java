package com.redismq.common.serializer;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.jsr310.deser.LocalDateDeserializer;
import com.fasterxml.jackson.datatype.jsr310.deser.LocalDateTimeDeserializer;
import com.fasterxml.jackson.datatype.jsr310.ser.LocalDateSerializer;
import com.fasterxml.jackson.datatype.jsr310.ser.LocalDateTimeSerializer;
import com.redismq.common.pojo.Message;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;


/**
 * @author hzh
 * @date 2021/4/4 16:57 更新
 * Json序列化工具类  实例化速度慢
 */
public class RedisMQStringMapper {
    // 定义jackson对象
    
    public static final ObjectMapper STRING_MAPPER = new ObjectMapper();
    static {
        //在解析json的时候忽略字段名字不对应的会报错的情况  如usernamexxx字段映射到User实体类
        STRING_MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        //为空的列不参与序列化   以免es更新的时候多更新了null的列
        STRING_MAPPER.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        // 忽略 transient 修饰的属性
        STRING_MAPPER.configure(MapperFeature.PROPAGATE_TRANSIENT_MARKER, true);
        //解决jackson2无法反序列化LocalDateTime的问题
        STRING_MAPPER.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        // 指定要序列化的域，field,get和set,以及修饰符范围，ANY是都有包括private和public
        SimpleModule simpleModule = new SimpleModule();
        simpleModule.addSerializer(Message.class,new MessageSerializer(Message.class));
        simpleModule.addDeserializer(Message.class,new MessageDeserializer(Message.class));
        simpleModule.addDeserializer(LocalDateTime.class, new LocalDateTimeDeserializer(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
        simpleModule.addSerializer(LocalDateTime.class, new LocalDateTimeSerializer(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
        simpleModule.addSerializer(LocalDate.class, new LocalDateSerializer(DateTimeFormatter.ofPattern("yyyy-MM-dd")));
        simpleModule.addDeserializer(LocalDate.class, new LocalDateDeserializer(DateTimeFormatter.ofPattern("yyyy-MM-dd")));
  
        STRING_MAPPER.setVisibility(PropertyAccessor.ALL, JsonAutoDetect.Visibility.ANY);
        STRING_MAPPER.registerModule(simpleModule);
    }
    
//    public static void main(String[] args) {
//        ConcurrentMap<Object, Object> objectObjectConcurrentMap = Maps.newConcurrentMap();
//        objectObjectConcurrentMap.put("a","b");
//        Message message = new Message();
//        message.setBody(objectObjectConcurrentMap);
//        message.setVirtualQueueName("dadsads");
//        String s = RedisMQStringMapper.toJsonStr(message);
//        System.out.println(s);
//        Message message1 = RedisMQStringMapper.toBean("{\"body\":{\"a\":\"b\"},\"virtualQueueName\":\"dadsads\"}", Message.class);
//        System.out.println(message1);
//    }


    public static <T> T mapToBean(Map<String, Object> source, Class<T> targetType) {
        try {
            String json = toJsonStr(source);
            T t = toBean(json, targetType);
            return t;
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }
    
    public static <T,K,V> List<T> mapsToBeans(List<Map<K,V>> source, Class<T> targetType) {
        try {
            String json = toJsonStr(source);
            List<T> ts = toList(json, targetType);
            return ts;
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    // 将对象转换成json字符串
    public static String toJsonStr(Object obj) {
        if (obj instanceof String){
            return obj.toString();
        }
        try {
            return STRING_MAPPER.writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    // 将json数据转换成pojo对象
    public static <T> T toBean(String json, Class<T> beanType) {
        try {
            T t = STRING_MAPPER.readValue(json, beanType);
            return t;
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }
    public static <T> T toBean(byte[] bytes, Class<T> beanType) {
        try {
            T t = STRING_MAPPER.readValue(bytes, beanType);
            return t;
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public static Map<String, Object> toMap(String json) {
        try {
            return STRING_MAPPER.readValue(json, Map.class);
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public static Map<String, Object> beanToMap(Object bean) {
        try {
            String json = STRING_MAPPER.writeValueAsString(bean);
            return toMap(json);
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    // 将byte数组转对象
    public static <T> T bytesToBean(byte[] bytes, Class<T> beanType) {
        String json = new String(bytes);

        try {
            T t = STRING_MAPPER.readValue(json, beanType);
            return t;
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    // 将json数据转换成pojo对象list
    public static <T> List<T> toList(String json, Class<T> beanType) {
        JavaType javaType = STRING_MAPPER.getTypeFactory().constructParametricType(List.class, beanType);
        try {
            return STRING_MAPPER.readValue(json, javaType);
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }
}