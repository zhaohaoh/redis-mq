package com.redismq.rpc.manager;

import com.redismq.common.pojo.AddressInfo;
import com.redismq.common.util.NetUtil;
import com.redismq.rpc.client.NettyClientBootstrap;
import io.netty.channel.Channel;
import org.apache.commons.pool2.KeyedPooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

/**
 * The type Netty key poolable factory.
 *
 */
public class NettyPoolableFactory implements KeyedPooledObjectFactory<AddressInfo, Channel> {
    
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
    public void activateObject(AddressInfo addressInfo, PooledObject<Channel> pooledObject) throws Exception {
    
    }
    
    @Override
    public void destroyObject(AddressInfo addressInfo, PooledObject<Channel> pooledObject) throws Exception {
        Channel channel = pooledObject.getObject();
        if (channel != null) {
            LOGGER.info("will destroy channel:" + channel);
            channel.disconnect();
            channel.close();
        }
    }
    
    @Override
    public PooledObject<Channel> makeObject(AddressInfo key) {
        InetSocketAddress address = NetUtil.toInetSocketAddress(key.getTargetAddress());
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("NettyPool create channel to " + key);
        }
        Channel newChannel = clientBootstrap.getNewChannel(address);
        return new DefaultPooledObject<>(newChannel);
    }
    
    @Override
    public void passivateObject(AddressInfo addressInfo, PooledObject<Channel> pooledObject) throws Exception {
    
    }
    
    @Override
    public boolean validateObject(AddressInfo addressInfo, PooledObject<Channel> pooledObject) {
        Channel obj = pooledObject.getObject();
        if (obj != null && obj.isActive()) {
            return true;
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("channel valid false,channel:" + obj);
        }
        return false;
    }
    
 
    
    
}
