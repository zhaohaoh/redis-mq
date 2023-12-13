package com.redismq.utils;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.redismq.Message;
import org.apache.commons.lang3.ArrayUtils;

import java.io.IOException;

/**
 * @author hzh
 * @date 2020/11/18 18:21 自定义刷新令牌json解析器
 */
public class MessageSerializer extends StdSerializer<Message> {
    
    protected MessageSerializer(Class<Message> vc) {
        super(vc);
    }
    
    /**
     * 序列化逻辑
     */
    @Override
    public void serialize(Message message, JsonGenerator jsonGenerator, SerializerProvider serializerProvider)
            throws IOException {
        // JsonGenerator中有很多有用的数据，比如gen.getOutputContext()可以拿到原始的字段属性
        // 调用JsonGenerator的write开头的方法以写入序列化后的数据
        jsonGenerator.writeStartObject();
        if (message.getBody() instanceof String) {
            String bodyStr = message.getBody().toString();
            if (isJson(bodyStr)) {
                bodyStr = removeAll(bodyStr, '\r', '\n', ' ');
                jsonGenerator.writeFieldName("body");
                jsonGenerator.writeRawValue(bodyStr);
            } else {
                jsonGenerator.writeStringField("body", bodyStr);
            }
        } else {
            jsonGenerator.writeObjectField("body", message.getBody());
        }
        
        jsonGenerator.writeStringField("id", message.getId());
        jsonGenerator.writeStringField("key", message.getKey());
        jsonGenerator.writeStringField("queue", message.getQueue());
        jsonGenerator.writeStringField("tag", message.getTag());
        jsonGenerator.writeStringField("virtualQueueName", message.getVirtualQueueName());
        jsonGenerator.writeEndObject();
    }
    
    private boolean isJson(String bodyStr) {
        return isWrap(bodyStr.trim(), '{', '}') || isWrap(bodyStr.trim(), '[', ']');
    }
    
    public boolean isWrap(CharSequence str, char prefixChar, char suffixChar) {
        if (null == str) {
            return false;
        }
        
        return str.charAt(0) == prefixChar && str.charAt(str.length() - 1) == suffixChar;
    }
    
    //移除字符
    public static String removeAll(CharSequence str, char... chars) {
        if (null == str || ArrayUtils.isEmpty(chars)) {
            return str(str);
        }
        final int len = str.length();
        if (0 == len) {
            return str(str);
        }
        final StringBuilder builder = new StringBuilder(len);
        char c;
        for (int i = 0; i < len; i++) {
            c = str.charAt(i);
            if (false == ArrayUtils.contains(chars, c)) {
                builder.append(c);
            }
        }
        return builder.toString();
    }
    
    public static String str(CharSequence cs) {
        return null == cs ? null : cs.toString();
    }
}
