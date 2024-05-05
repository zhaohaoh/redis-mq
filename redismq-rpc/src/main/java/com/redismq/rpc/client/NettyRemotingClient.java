package com.redismq.rpc.client;

import com.redismq.rpc.manager.NettyClientChannelManager;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class NettyRemotingClient extends AbstractNettyRemoting implements RemotingClient {
    
    
    public NettyRemotingClient(NettyClientChannelManager nettyClientChannelManager) {
        super(nettyClientChannelManager);
    }
}
