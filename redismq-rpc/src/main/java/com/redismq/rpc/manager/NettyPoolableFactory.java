package com.redismq.rpc.manager;

import com.redismq.common.util.NetUtil;
import com.redismq.rpc.client.NettyClientBootstrap;
import com.redismq.common.pojo.AddressInfo;
import io.netty.channel.Channel;
import org.apache.commons.pool.KeyedPoolableObjectFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

/**
 * The type Netty key poolable factory.
 *
 */
public class NettyPoolableFactory implements KeyedPoolableObjectFactory<AddressInfo, Channel> {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(NettyPoolableFactory.class);
    
    
    private final NettyClientBootstrap clientBootstrap;
    
    /**
     * Instantiates a new Netty key poolable factory.
     *
     */
    public NettyPoolableFactory( NettyClientBootstrap clientBootstrap) {
        this.clientBootstrap = clientBootstrap;
    }
    
    @Override
    public Channel makeObject(AddressInfo key) {
        InetSocketAddress address = NetUtil.toInetSocketAddress(key.getTargetAddress());
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("NettyPool create channel to " + key);
        }
        return clientBootstrap.getNewChannel(address);
    }
    
    @Override
    public void destroyObject(AddressInfo key, Channel channel) throws Exception {
        if (channel != null) {
            LOGGER.info("will destroy channel:" + channel);
            channel.disconnect();
            channel.close();
        }
    }
    
    @Override
    public boolean validateObject(AddressInfo key, Channel obj) {
        if (obj != null && obj.isActive()) {
            return true;
        }
        LOGGER.info("channel valid false,channel:" + obj);
        return false;
    }
    
    @Override
    public void activateObject(AddressInfo key, Channel obj) throws Exception {
    
    }
    
    @Override
    public void passivateObject(AddressInfo key, Channel obj) throws Exception {
    
    }
}
