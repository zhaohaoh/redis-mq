//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package com.redismq.autoconfigure;

import com.redismq.config.RedisProperties;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;
import org.redisson.config.ClusterServersConfig;
import org.redisson.config.Config;
import org.redisson.config.SentinelServersConfig;
import org.redisson.config.SingleServerConfig;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Method;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class RedissonMQClient {
 
  
    public static RedissonClient redisMQRedisson(RedisProperties redisProperties)   {
        Method clusterMethod = ReflectionUtils.findMethod(RedisProperties.class, "getCluster");
        Method usernameMethod = ReflectionUtils.findMethod(RedisProperties.class, "getUsername");
        Method timeoutMethod = ReflectionUtils.findMethod(RedisProperties.class, "getTimeout");
        Method connectTimeoutMethod = ReflectionUtils.findMethod(RedisProperties.class, "getConnectTimeout");
        Method clientNameMethod = ReflectionUtils.findMethod(RedisProperties.class, "getClientName");
        Object timeoutValue = ReflectionUtils.invokeMethod(timeoutMethod,  redisProperties);
        Integer timeout = null;
        if (timeoutValue instanceof Duration) {
            timeout = (int)((Duration)timeoutValue).toMillis();
        } else if (timeoutValue != null) {
            timeout = (Integer)timeoutValue;
        }

        Integer connectTimeout = null;
        if (connectTimeoutMethod != null) {
            Object connectTimeoutValue = ReflectionUtils.invokeMethod(connectTimeoutMethod,  redisProperties);
            if (connectTimeoutValue != null) {
                connectTimeout = (int)((Duration)connectTimeoutValue).toMillis();
            }
        } else {
            connectTimeout = timeout;
        }

        String clientName = null;
        if (clientNameMethod != null) {
            clientName = (String)ReflectionUtils.invokeMethod(clientNameMethod,  redisProperties);
        }

        String username = null;
        if (usernameMethod != null) {
            username = (String)ReflectionUtils.invokeMethod(usernameMethod,  redisProperties);
        }

        Config config;
        if ( redisProperties.getSentinel() != null) {
            Method nodesMethod = ReflectionUtils.findMethod(RedisProperties.Sentinel.class, "getNodes");
            Object nodesValue = ReflectionUtils.invokeMethod(nodesMethod,  redisProperties.getSentinel());
            String[] nodes;
            if (nodesValue instanceof String) {
                nodes =  convert(Arrays.asList(((String)nodesValue).split(",")));
            } else {
                nodes =  convert((List)nodesValue);
            }

            config = new Config();
            SentinelServersConfig c = config.useSentinelServers().setMasterName( redisProperties.getSentinel().getMaster()).addSentinelAddress(nodes).setDatabase( redisProperties.getDatabase()).setUsername(username)
                    .setPassword( redisProperties.getPassword())
                    .setClientName(clientName);
            if (connectTimeout != null) {
                c.setConnectTimeout(connectTimeout);
            }

            if (connectTimeoutMethod != null && timeout != null) {
                c.setTimeout(timeout);
            }
        } else {
            Method method;
            if (clusterMethod != null && ReflectionUtils.invokeMethod(clusterMethod,  redisProperties) != null) {
                Object clusterObject = ReflectionUtils.invokeMethod(clusterMethod,  redisProperties);
                method = ReflectionUtils.findMethod(clusterObject.getClass(), "getNodes");
                List<String> nodesObject = (List)ReflectionUtils.invokeMethod(method, clusterObject);
                String[] nodes =  convert(nodesObject);
                config = new Config();
                ClusterServersConfig c =  config.useClusterServers().addNodeAddress(nodes).setUsername(username).setPassword( redisProperties.getPassword())
                        .setClientName(clientName);
                if (connectTimeout != null) {
                    c.setConnectTimeout(connectTimeout);
                }

                if (connectTimeoutMethod != null && timeout != null) {
                    c.setTimeout(timeout);
                }
            } else {
                config = new Config();
                String prefix = "redis://";
                method = ReflectionUtils.findMethod(RedisProperties.class, "isSsl");
                if (method != null && (Boolean)ReflectionUtils.invokeMethod(method,  redisProperties)) {
                    prefix = "rediss://";
                }

                SingleServerConfig c = config.useSingleServer().setAddress(prefix +  redisProperties.getHost() + ":" +  redisProperties.getPort()).setDatabase( redisProperties.getDatabase()).setUsername(username)
                        .setPassword( redisProperties.getPassword())
                        .setClientName(clientName);
                if (connectTimeout != null) {
                    c.setConnectTimeout(connectTimeout);
                }

                if (connectTimeoutMethod != null && timeout != null) {
                    c.setTimeout(timeout);
                }
            }
        }
        
//        JsonJacksonCodec jsonJacksonCodec = new JsonJacksonCodec(RedisMQStringMapper.STRING_MAPPER);
        config.setCodec(StringCodec.INSTANCE);
        RedissonClient redissonClient = Redisson.create(config);
    
        return redissonClient;
    }

    private static String[] convert(List<String> nodesObject) {
        List<String> nodes = new ArrayList(nodesObject.size());
        Iterator var3 = nodesObject.iterator();

        while(true) {
            while(var3.hasNext()) {
                String node = (String)var3.next();
                if (!node.startsWith("redis://") && !node.startsWith("rediss://")) {
                    nodes.add("redis://" + node);
                } else {
                    nodes.add(node);
                }
            }

            return (String[])nodes.toArray(new String[0]);
        }
    }
 
}
