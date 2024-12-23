package com.redismq.common.serializer;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.node.MissingNode;
import com.redismq.common.pojo.Message;

import java.io.IOException;
import java.util.Map;

/**
 * @author hzh
 * @date 2020/11/18 18:21 自定义刷新令牌json解析器
 */
public class MessageDeserializer extends StdDeserializer<Message> {
    
    protected MessageDeserializer(Class<?> vc) {
        super(vc);
    }
    
    @Override
    public Message deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
            throws IOException {
        //获取ObjectMapper
        ObjectMapper mapper = (ObjectMapper) jsonParser.getCodec();
        //读取节点
        JsonNode jsonNode = mapper.readTree(jsonParser);
        //jsonNode获取字段
        JsonNode body = jsonNode.has("body") ? jsonNode.get("body") : MissingNode.getInstance();
        JsonNode id = jsonNode.has("id") ? jsonNode.get("id") : MissingNode.getInstance();
        JsonNode key = jsonNode.has("key") ? jsonNode.get("key") : MissingNode.getInstance();
        JsonNode queue = jsonNode.has("queue") ? jsonNode.get("queue") : MissingNode.getInstance();
        JsonNode tag = jsonNode.has("tag") ? jsonNode.get("tag") : MissingNode.getInstance();
        JsonNode offset = jsonNode.has("offset") ? jsonNode.get("offset") : MissingNode.getInstance();
        JsonNode executeTime = jsonNode.has("executeTime") ? jsonNode.get("executeTime") : MissingNode.getInstance();
        JsonNode executorScope = jsonNode.has("executorScope") ? jsonNode.get("executorScope") : MissingNode.getInstance();
        JsonNode virtualQueueName =
                jsonNode.has("virtualQueueName") ? jsonNode.get("virtualQueueName") : MissingNode.getInstance();
        JsonNode header = jsonNode.has("header") ? jsonNode.get("header") : MissingNode.getInstance();
        
        Message message = new Message();
        // 现在你可以使用JsonNode的方法来确定它的具体类型
        if (!body.isMissingNode()) {
            //默认转字符串
            Object obj = body.toString();
            if (body.isObject()) {
                obj = body.toString();
            }
            else if (body.isArray()) {
                obj = body.toString();
            }
            else if (body.isPojo()) {
                obj = body.toString();
            }
            else if (body.isDouble()) {
                obj = body.asDouble();
                // 处理数组节点
            } else if (body.isFloat()) {
                obj = body.floatValue();
            } else if (body.isBinary()) {
                obj = body.binaryValue();
            } else if (body.isBigDecimal()) {
                obj = body.isBigDecimal();
            } else if (body.isBigInteger()) {
                obj = body.bigIntegerValue();
            } else if (body.isTextual()) {
                obj = body.asText();
                // 处理文本值
            }
            else if (body.isLong()) {
                obj = body.asLong();
                // 处理布尔值
            }
            else if (body.isInt()) {
                obj = body.asInt();
                // 处理数值
            } else if (body.isBoolean()) {
                obj = body.asBoolean();
                // 处理布尔值
            }  else if (body.isShort()) {
                obj = body.shortValue();
                // 处理布尔值
            }
            else if (body.isNull()) {
                obj = null;
                // 处理null值
            }
            message.setBody(obj);
            
        }
        message.setId(id.asText());
        message.setKey(key.asText());
        message.setQueue(queue.asText());
        message.setTag(tag.asText());
        message.setOffset(offset.asLong());
        message.setExecuteTime(executeTime.asLong());
        message.setVirtualQueueName(virtualQueueName.asText());
        message.setExecuteScope(executorScope.asLong());
        if (!header.isMissingNode()) {
            JsonParser traverse = header.traverse();
            String valueAsString = traverse.getValueAsString();
            if (valueAsString!=null) {
                Map<String, Object> map = mapper.readValue(traverse, Map.class);
                message.setHeader(map);
            }
        }
        
        return message;
    }
}
