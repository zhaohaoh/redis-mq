package com.redismq.utils;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.node.MissingNode;
import com.redismq.Message;
import java.io.IOException;

/**
 * @author hzh
 * @date 2020/11/18 18:21
 * 自定义刷新令牌json解析器
 */
public class MessageDeserializer extends StdDeserializer<Message> {
    protected MessageDeserializer(Class<?> vc) {
        super(vc);
    }

    @Override
    public Message deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException {
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
        JsonNode virtualQueueName = jsonNode.has("virtualQueueName") ? jsonNode.get("virtualQueueName") : MissingNode.getInstance();

        Message message = new Message();
        //如果是普通字符串转为字符串 。否则是json转换为json字符串
        String str = body.isTextual() ? body.asText() : body.toString();
        message.setBody(str);
        message.setId(id.asText());
        message.setKey(key.asText());
        message.setQueue(queue.asText());
        message.setTag(tag.asText());
        message.setVirtualQueueName(virtualQueueName.asText());

        return message;
    }
}
