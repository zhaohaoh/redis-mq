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
import org.springframework.util.StringUtils;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class RedissonMQClient {


    public static RedissonClient redisMQRedisson(RedisProperties redisProperties) {
        int timeout = (int) (Objects.isNull(redisProperties.getTimeout()) ? Duration.ofSeconds(30).toMillis() : redisProperties.getTimeout().toMillis());

        Integer connectTimeout = Objects.isNull(redisProperties.getConnectTimeout()) ? null : (int) redisProperties.getConnectTimeout().toMillis();

        String clientName = redisProperties.getClientName();

        String username = redisProperties.getUsername();

        Config config;
        if (Objects.nonNull(redisProperties.getSentinel()) && redisProperties.getSentinel().getNodes() != null && redisProperties.getSentinel().getNodes().size() > 0) {
            List<String> sentinelNodes = redisProperties.getSentinel().getNodes();
            String[] nodes = convert(sentinelNodes);

            config = new Config();
            SentinelServersConfig c = config.useSentinelServers().setMasterName(redisProperties.getSentinel().getMaster()).addSentinelAddress(nodes).setDatabase(redisProperties.getDatabase()).setClientName(clientName);
            if (StringUtils.hasText(username)) {
                config.setUsername(username);
            }
            if (StringUtils.hasText(redisProperties.getPassword())) {
                config.setPassword(redisProperties.getPassword());
            }

            if (connectTimeout != null) {
                c.setConnectTimeout(connectTimeout);
            }
            c.setTimeout(timeout);
        } else if (Objects.nonNull(redisProperties.getCluster()) && redisProperties.getCluster().getNodes() != null && redisProperties.getCluster().getNodes().size() > 0) {
            List<String> clusterNodes = redisProperties.getCluster().getNodes();
            String[] nodes = convert(clusterNodes);
            config = new Config();
            ClusterServersConfig c = config.useClusterServers().addNodeAddress(nodes).setClientName(clientName);

            if (StringUtils.hasText(username)) {
                config.setUsername(username);
            }
            if (StringUtils.hasText(redisProperties.getPassword())) {
                config.setPassword(redisProperties.getPassword());
            }

            if (connectTimeout != null) {
                c.setConnectTimeout(connectTimeout);
            }
            c.setTimeout(timeout);
        } else {
            config = new Config();
            String prefix = redisProperties.isSsl() ? "rediss://" : "redis://";

            SingleServerConfig c = config.useSingleServer().setAddress(prefix + redisProperties.getHost() + ":" + redisProperties.getPort()).setDatabase(redisProperties.getDatabase())
                    .setClientName(clientName);

            if (StringUtils.hasText(username)) {
                config.setUsername(username);
            }
            if (StringUtils.hasText(redisProperties.getPassword())) {
                config.setPassword(redisProperties.getPassword());
            }

            if (connectTimeout != null) {
                c.setConnectTimeout(connectTimeout);
            }
            c.setTimeout(timeout);
        }

//        JsonJacksonCodec jsonJacksonCodec = new JsonJacksonCodec(RedisMQStringMapper.STRING_MAPPER);
        config.setCodec(StringCodec.INSTANCE);
        RedissonClient redissonClient = Redisson.create(config);

        return redissonClient;
    }

    private static String[] convert(List<String> nodesObject) {
        List<String> nodes = new ArrayList<>(nodesObject.size());
        for (String node : nodesObject) {
            if (!node.startsWith("redis://") && !node.startsWith("rediss://")) {
                nodes.add("redis://" + node);
            } else {
                nodes.add(node);
            }
        }
        return nodes.toArray(new String[0]);
    }

}
