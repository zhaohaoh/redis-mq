package com.redismq.rpc.codec;

import com.redismq.common.pojo.AddressInfo;
import com.redismq.common.pojo.RemoteMessage;
import com.redismq.common.serializer.RedisMQStringMapper;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

/**
 * <pre>
 * 0     1     2     3     4     5     6     7     8     9    10     11    12    13    14    15    16
 * +-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+
 * |   magic   |Proto|     Full length       |    Head   | Msg |Seria|Compr|     RequestId         |
 * |   code    |colVer|    (head+body)      |   Length  |Type |lizer|ess  |                       |
 * +-----------+-----------+-----------+-----------+-----------+-----------+-----------+-----------+
 * |                                                                                               |
 * |                                   Head Map [Optional]                                         |
 * +-----------+-----------+-----------+-----------+-----------+-----------+-----------+-----------+
 * |                                                                                               |
 * |                                         body                                                  |
 * |                                                                                               |
 * |                                        ... ...                                                |
 * +-----------------------------------------------------------------------------------------------+
 * </pre>
 * <p>
 * <li>Full Length: include all data </li>
 * <li>Head Length: include head data from magic code to head map. </li>
 * <li>Body Length: Full Length - Head Length</li>
 * </p>
 * https://github.com/seata/seata/issues/893
 *
 * @author Geng Zhang
 * @see ProtocolV1Decoder
 * @since 0.7.0
 */
public class ProtocolV1Encoder extends MessageToByteEncoder {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(ProtocolV1Encoder.class);
    
    @Override
    public void encode(ChannelHandlerContext ctx, Object msg, ByteBuf out) {
        try {
            if (msg instanceof RemoteMessage) {
                RemoteMessage rpcMessage = (RemoteMessage) msg;
                
                int fullLength;
                int msgIdLength;
                int addressLength;
                int bodyLength;
                
                AddressInfo addressInfo = rpcMessage.getAddressInfo();
                String str = RedisMQStringMapper.toJsonStr(addressInfo);
                byte[] address = str.getBytes(StandardCharsets.UTF_8);
                
                addressLength=address.length;
                byte[] idBytes = rpcMessage.getId().getBytes(StandardCharsets.UTF_8);
                msgIdLength = idBytes.length;
                
                String body = rpcMessage.getBody();
                byte[]  bodyBytes = body.getBytes(StandardCharsets.UTF_8);
                bodyLength=bodyBytes.length;
                
                int messageType = rpcMessage.getMessageType();
                out.writeBytes(new byte[] {(byte) 0xda, (byte) 0xda});
                out.writerIndex(out.writerIndex() + 4);
                // full Length(4B) and head length(2B) will fix in the end. 
               
                out.writeInt(messageType);
                out.writeInt(msgIdLength);
                out.writeInt(addressLength);
                out.writeInt(bodyLength);
                out.writeBytes(idBytes);
                out.writeBytes(address);
                
                
                //魔数 + 4位长度 + 4位消息类型 +4位消息id长度+4位地址长度+4位消息体长度 + 消息id长度 + 消息地址长度 + 消息体长度
                fullLength = 2 + 4 + 4 + 4 + 4 + 4 + msgIdLength + addressLength + bodyLength;
                
                out.writeBytes(bodyBytes);
    
                //当前写入位置
                int writeIndex = out.writerIndex();
                //回到开始的第二个位置
                out.writerIndex(writeIndex - fullLength + 2);
                //然后写入消息总长度
                out.writeInt(fullLength);
                //回到最后位置
                out.writerIndex(writeIndex);
        
            } else {
                throw new UnsupportedOperationException("Not support this class:" + msg.getClass());
            }
        } catch (Throwable e) {
            LOGGER.error("Encode request error!", e);
        }
    }
}
