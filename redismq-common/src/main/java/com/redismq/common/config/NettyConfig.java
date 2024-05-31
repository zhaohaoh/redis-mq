package com.redismq.common.config;

import com.redismq.common.constant.TransportProtocolType;
import com.redismq.common.constant.TransportServerType;
import io.netty.channel.Channel;
import io.netty.channel.ServerChannel;
import io.netty.channel.epoll.EpollDomainSocketChannel;
import io.netty.channel.epoll.EpollServerDomainSocketChannel;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.kqueue.KQueueDomainSocketChannel;
import io.netty.channel.kqueue.KQueueServerDomainSocketChannel;
import io.netty.channel.kqueue.KQueueServerSocketChannel;
import io.netty.channel.kqueue.KQueueSocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.internal.PlatformDependent;
import lombok.Data;

import java.time.Duration;

@Data
public class NettyConfig {
    /**
     * rpc通信超时时间 传递消息超时时间
     */
    private long rpcRequestTimeout = Duration.ofSeconds(30).toMillis();
    /**
     * 轮询批量发送消息的阻塞时间 调大会增加发消息的延迟  调小会增加cpu消耗
     */
    private int maxMergeSendMills = 100;
    /**
     * The constant TRANSPORT_SERVER_TYPE.
     */
    protected TransportServerType transportServerType = TransportServerType.NIO;
    
    /**
     * The constant SERVER_CHANNEL_CLAZZ.
     */
    protected Class<? extends ServerChannel> serverChannelClazz = NioServerSocketChannel.class;
    
    /**
     * The constant CLIENT_CHANNEL_CLAZZ.
     */
    protected Class<? extends Channel> clientChannelClazz = NioSocketChannel.class;
    
    /**
     * The constant TRANSPORT_PROTOCOL_TYPE.
     */
    protected TransportProtocolType transportProtocolType = TransportProtocolType.TCP;
    
    /**
     * 心跳读间隔
     */
    protected int heartbeatReadSeconds = 15;
    
    /**
     * 心跳写间隔
     */
    protected int heartbeatWriteSeconds = 5;
    
    /**
     * 服务端配置
     */
    protected Server server;
    /**
     * 客户端配置
     */
    protected Client client;
    
 
    public void init() {
        switch (transportServerType) {
            case  NIO:
                if (transportProtocolType == TransportProtocolType.TCP) {
                    serverChannelClazz = NioServerSocketChannel.class;
                    clientChannelClazz = NioSocketChannel.class;
                } else {
                    serverChannelClazz = null;
                    clientChannelClazz = null;
                }
                break;
            case  NATIVE:
                if (PlatformDependent.isWindows()) {
                    throw new IllegalArgumentException("no native supporting for Windows.");
                } else if (PlatformDependent.isOsx()) {
                    if (transportProtocolType == TransportProtocolType.TCP) {
                        serverChannelClazz = KQueueServerSocketChannel.class;
                        clientChannelClazz = KQueueSocketChannel.class;
                    } else if (transportProtocolType == TransportProtocolType.UNIX_DOMAIN_SOCKET) {
                        serverChannelClazz = KQueueServerDomainSocketChannel.class;
                        clientChannelClazz = KQueueDomainSocketChannel.class;
                    } else {
                        serverChannelClazz = null;
                        clientChannelClazz = null;
                    }
                } else {
                    if (transportProtocolType == TransportProtocolType.TCP) {
                        serverChannelClazz = EpollServerSocketChannel.class;
                        clientChannelClazz = EpollSocketChannel.class;
                    } else if (transportProtocolType == TransportProtocolType.UNIX_DOMAIN_SOCKET) {
                        serverChannelClazz = EpollServerDomainSocketChannel.class;
                        clientChannelClazz = EpollDomainSocketChannel.class;
                    } else {
                        serverChannelClazz = null;
                        clientChannelClazz = null;
                    }
                }
                break;
            default:
                throw new IllegalArgumentException("unsupported.");
        }
    }
    
    @Data
    public static class Server {
        /**
         * 启动服务端
         */
        private boolean enable = true;
        /**
         * 发送缓冲区大小
         */
        private int serverSocketSendBufSize = 153600;
        /**
         * 接受缓冲区大小
         */
        private int serverSocketResvBufSize = 153600;
        /**
         * 服务端连接队列大小
         */
        private int soBackLogSize = 1024;
        /**
         * 写缓存区的高低水位线
         */
        private int writeBufferHighWaterMark = 67108864;
        
        private int writeBufferLowWaterMark = 1048576;
        /**
         * 默认监听端口
         */
        private int port = 10520;

        /**
         * 服务端处理消息的线程池大小
         */
//        private int minServerPoolSize = 50;
//
//        private int maxServerPoolSize = 500;
       
        private int serverRegisterExpireSeconds = 5;
    }
    
    @Data
    public static class Client {
        /**
         * 启动客户端
         */
        private boolean enable = false;
        /**
         * 连接服务端超时时间
         */
        private int connectTimeoutMillis = 10000;
        /**
         * 拉取服务超时时间
         */
        private int selectServerTimeoutMillis = 10000;
        /**
         * 健康检测
         */
        private boolean health = false;
        /**
         * 发送缓冲区大小  150 KB
         */
        private int clientSocketSndBufSize = 153600;
        /**
         * 接受缓存区大小  150 KB
         */
        private int clientSocketRcvBufSize = 153600;
    
        /**
         * The constant WORKER_THREAD_SIZE.
         */
        protected int clientThreadSize;
    }
    
}
