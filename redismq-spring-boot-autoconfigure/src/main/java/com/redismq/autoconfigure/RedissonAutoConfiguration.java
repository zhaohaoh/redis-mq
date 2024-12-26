//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package com.redismq.autoconfigure;

import com.redismq.config.RedisProperties;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.api.RedissonReactiveClient;
import org.redisson.api.RedissonRxClient;
import org.redisson.client.codec.StringCodec;
import org.redisson.config.ClusterServersConfig;
import org.redisson.config.Config;
import org.redisson.config.SentinelServersConfig;
import org.redisson.config.SingleServerConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.util.ReflectionUtils;

import java.io.IOException;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

@Configuration
@AutoConfigureBefore({RedisAutoConfiguration.class})
@EnableConfigurationProperties({ RedisProperties.class})
public class RedissonAutoConfiguration {
    
    @Autowired
    private RedisProperties redisProperties;

    public RedissonAutoConfiguration() {
    }
    

    @Bean
    @Lazy
    @ConditionalOnMissingBean({RedissonReactiveClient.class})
    public RedissonReactiveClient redissonReactive(RedissonClient redisson) {
        return redisson.reactive();
    }

    @Bean
    @Lazy
    @ConditionalOnMissingBean({RedissonRxClient.class})
    public RedissonRxClient redissonRxJava(RedissonClient redisson) {
        return redisson.rxJava();
    }

    @Bean(
        destroyMethod = "shutdown"
    )
    @ConditionalOnMissingBean({RedissonClient.class})
    public RedissonClient redisson() throws IOException {
        Method clusterMethod = ReflectionUtils.findMethod(RedisProperties.class, "getCluster");
        Method usernameMethod = ReflectionUtils.findMethod(RedisProperties.class, "getUsername");
        Method timeoutMethod = ReflectionUtils.findMethod(RedisProperties.class, "getTimeout");
        Method connectTimeoutMethod = ReflectionUtils.findMethod(RedisProperties.class, "getConnectTimeout");
        Method clientNameMethod = ReflectionUtils.findMethod(RedisProperties.class, "getClientName");
        Object timeoutValue = ReflectionUtils.invokeMethod(timeoutMethod, this.redisProperties);
        Integer timeout = null;
        if (timeoutValue instanceof Duration) {
            timeout = (int)((Duration)timeoutValue).toMillis();
        } else if (timeoutValue != null) {
            timeout = (Integer)timeoutValue;
        }

        Integer connectTimeout = null;
        if (connectTimeoutMethod != null) {
            Object connectTimeoutValue = ReflectionUtils.invokeMethod(connectTimeoutMethod, this.redisProperties);
            if (connectTimeoutValue != null) {
                connectTimeout = (int)((Duration)connectTimeoutValue).toMillis();
            }
        } else {
            connectTimeout = timeout;
        }

        String clientName = null;
        if (clientNameMethod != null) {
            clientName = (String)ReflectionUtils.invokeMethod(clientNameMethod, this.redisProperties);
        }

        String username = null;
        if (usernameMethod != null) {
            username = (String)ReflectionUtils.invokeMethod(usernameMethod, this.redisProperties);
        }

        Config config;
        if (this.redisProperties.getSentinel() != null) {
            Method nodesMethod = ReflectionUtils.findMethod(RedisProperties.Sentinel.class, "getNodes");
            Object nodesValue = ReflectionUtils.invokeMethod(nodesMethod, this.redisProperties.getSentinel());
            String[] nodes;
            if (nodesValue instanceof String) {
                nodes = this.convert(Arrays.asList(((String)nodesValue).split(",")));
            } else {
                nodes = this.convert((List)nodesValue);
            }

            config = new Config();
            SentinelServersConfig c = config.useSentinelServers().setMasterName(this.redisProperties.getSentinel().getMaster()).addSentinelAddress(nodes).setDatabase(this.redisProperties.getDatabase()).setUsername(username)
                    .setPassword(this.redisProperties.getPassword())
                    .setClientName(clientName);
            if (connectTimeout != null) {
                c.setConnectTimeout(connectTimeout);
            }

            if (connectTimeoutMethod != null && timeout != null) {
                c.setTimeout(timeout);
            }
        } else {
            Method method;
            if (clusterMethod != null && ReflectionUtils.invokeMethod(clusterMethod, this.redisProperties) != null) {
                Object clusterObject = ReflectionUtils.invokeMethod(clusterMethod, this.redisProperties);
                method = ReflectionUtils.findMethod(clusterObject.getClass(), "getNodes");
                List<String> nodesObject = (List)ReflectionUtils.invokeMethod(method, clusterObject);
                String[] nodes = this.convert(nodesObject);
                config = new Config();
                ClusterServersConfig c =  config.useClusterServers().addNodeAddress(nodes).setUsername(username).setPassword(this.redisProperties.getPassword())
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
                if (method != null && (Boolean)ReflectionUtils.invokeMethod(method, this.redisProperties)) {
                    prefix = "rediss://";
                }

                SingleServerConfig c = config.useSingleServer().setAddress(prefix + this.redisProperties.getHost() + ":" + this.redisProperties.getPort()).setDatabase(this.redisProperties.getDatabase()).setUsername(username)
                        .setPassword(this.redisProperties.getPassword())
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

    private String[] convert(List<String> nodesObject) {
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
