package com.redismq.common.serializer;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.redismq.common.pojo.Message;
import org.apache.commons.lang3.ArrayUtils;

import java.io.IOException;
import java.util.Map;

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
            if (JsonSerializerUtil.isJson(bodyStr)) {
                bodyStr = removeWhitespacesExceptInQuotes(bodyStr);
                jsonGenerator.writeFieldName("body");
                jsonGenerator.writeRawValue(bodyStr);
            } else {
                jsonGenerator.writeStringField("body", bodyStr);
            }
        } else {
            jsonGenerator.writeObjectField("body", message.getBody());
        }
        jsonGenerator.writeObjectField("offset", message.getOffset());
        jsonGenerator.writeStringField("id", message.getId());
        jsonGenerator.writeStringField("key", message.getKey());
        jsonGenerator.writeStringField("queue", message.getQueue());
        jsonGenerator.writeStringField("tag", message.getTag());
        jsonGenerator.writeStringField("virtualQueueName", message.getVirtualQueueName());
        jsonGenerator.writeObjectField("executeTime",message.getExecuteTime());
        jsonGenerator.writeObjectField("executorScope",message.getExecuteScope());
        
        Map<String, Object> header = message.getHeader();
        if (header != null) {
            jsonGenerator.writeObjectField("header", header);
        }
        jsonGenerator.writeEndObject();
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
    
    public static void main(String[] args) {
        String string = "{\n"
                + "    \"className\": \"com.sjaco.floweryhy.api.controller.agentApp.sku.SkuInfoAgentAppController\",\n"
                + "    \"ctime\": \"2025-02-05 17:10:56\",\n" + "    \"groupName\": \"AgentSku\",\n"
                + "    \"id\": \"2824973\",\n" + "    \"key\": \"skuId\",\n" + "    \"keyValue\": \"99340\",\n"
                + "    \"logName\": \"修改商品价格库存\",\n" + "    \"logType\": \"业务日志\",\n"
                + "    \"method\": \"updateStockPrice\",\n"
                + "    \"msg\": \"[{\\\"skuId\\\":\\\"99340\\\",\\\"stock\\\":5}]\",\n" + "    \"name\": \"木龙\",\n"
                + "    \"phoneBrand\": \"oppo\",\n" + "    \"phoneDeviceId\": \"17289872285985797647\",\n"
                + "    \"phoneModel\": \"PHQ110\",\n" + "    \"succeed\": \"成功\",\n"
                + "    \"userId\": \"10018013\"\n" + "}";
        
        String s1 = removeWhitespacesExceptInQuotes(string);
        System.out.println(s1);
    }
    
    
    public static String removeWhitespacesExceptInQuotes(String input) {
        // Use a StringBuilder to build the result
        StringBuilder result = new StringBuilder();
        int length = input.length();
        int i = 0;
        boolean inQuotes = false;
        
        while (i < length) {
            char currentChar = input.charAt(i);
            
            if (currentChar == '"') {
                // Toggle the inQuotes flag
                inQuotes = !inQuotes;
                result.append(currentChar);
            } else if (!inQuotes && Character.isWhitespace(currentChar)) {
                // Skip whitespace characters outside of quotes
            } else {
                result.append(currentChar);
            }
            
            i++;
        }
        
        return result.toString();
    }
    
   
}


