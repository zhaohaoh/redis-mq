
package com.redismq.rpc.codec;

import com.redismq.common.pojo.AddressInfo;
import com.redismq.common.pojo.RemoteMessage;
import com.redismq.common.serializer.RedisMQStringMapper;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ProtocolV1Decoder extends LengthFieldBasedFrameDecoder {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProtocolV1Decoder.class);
    
    public ProtocolV1Decoder() {
      
      this(64 * 1024 * 1024);
    }

    public ProtocolV1Decoder(int maxFrameLength) {
    
        /**
         * 1.偏移2位魔数，消息总长度从哪个偏移量开始算
         * 2.消息总长度4位
         * 3.矫正偏移量把魔数和长度数据加上。 2 + 4 = -6
         * 4.解包后的帧开头多少字节应该被去掉。
         */
        super(maxFrameLength, 2, 4, -6, 0);
    }

    @Override
    protected Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
        Object decoded;
        try {
            decoded = super.decode(ctx, in);
            if (decoded instanceof ByteBuf) {
                ByteBuf frame = (ByteBuf)decoded;
                try {
                    return decodeFrame(frame);
                } finally {
                    frame.release();
                }
            }
        } catch (Exception exx) {
            LOGGER.error("Decode frame error, cause: {}", exx.getMessage());
            throw new RuntimeException(exx);
        }
        return decoded;
    }

    public Object decodeFrame(ByteBuf frame) {
        byte b0 = frame.readByte();
        byte b1 = frame.readByte();
//        if (ProtocolConstants.MAGIC_CODE_BYTES[0] != b0
//                || ProtocolConstants.MAGIC_CODE_BYTES[1] != b1) {
//            throw new IllegalArgumentException("Unknown magic code: " + b0 + ", " + b1);
//        }

    

        int fullLength = frame.readInt();
        int msgType = frame.readInt();
        int msgIdLength = frame.readInt();
        int addressLength = frame.readInt();
        int bodyLength = frame.readInt();
    
        byte[] msgIdB = new byte[msgIdLength];
        frame.readBytes(msgIdB);
        String msgIdStr = new String(msgIdB);
        
        byte[] bs1 = new byte[addressLength];
        frame.readBytes(bs1);
        String addressStr = new String(bs1);
    
        byte[] bs = new byte[bodyLength];
        frame.readBytes(bs);
        String bodyStr = new String(bs);
    
        AddressInfo addressInfo = RedisMQStringMapper.toBean(addressStr, AddressInfo.class);
        RemoteMessage rpcMessage = new RemoteMessage();
        rpcMessage.setAddressInfo(addressInfo);
        rpcMessage.setId(msgIdStr);
        rpcMessage.setBody(bodyStr);
        rpcMessage.setMessageType(msgType);
        
        return rpcMessage;
    }
}
