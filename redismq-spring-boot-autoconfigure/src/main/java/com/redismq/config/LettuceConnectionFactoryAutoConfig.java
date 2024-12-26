package com.redismq.config;

import io.lettuce.core.ClientOptions;
import io.lettuce.core.SocketOptions;
import io.lettuce.core.TimeoutOptions;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.ClusterTopologyRefreshOptions;
import io.lettuce.core.resource.ClientResources;
import io.lettuce.core.resource.DefaultClientResources;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.data.redis.JedisClientConfigurationBuilderCustomizer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisClusterConfiguration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisSentinelConfiguration;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettucePoolingClientConfiguration;
import org.springframework.util.StringUtils;

import java.time.Duration;


@EnableConfigurationProperties({ RedisProperties.class})
@ConditionalOnProperty(name = "spring.redismq.client.client-type", havingValue = "lettuce", matchIfMissing = true)
@Configuration
public class LettuceConnectionFactoryAutoConfig extends RedisMQConnectionConfiguration{
    
    protected LettuceConnectionFactoryAutoConfig(RedisProperties properties,
            ObjectProvider<RedisStandaloneConfiguration> standaloneConfigurationProvider,
            ObjectProvider<RedisSentinelConfiguration> sentinelConfigurationProvider,
            ObjectProvider<RedisClusterConfiguration> clusterConfigurationProvider) {
        super(properties, standaloneConfigurationProvider, sentinelConfigurationProvider, clusterConfigurationProvider);
    }
    
     @Bean
     @ConditionalOnMissingBean(RedisConnectionFactory.class)
     LettuceConnectionFactory redisConnectionFactory(
            ObjectProvider<JedisClientConfigurationBuilderCustomizer> builderCustomizers) {
        LettuceConnectionFactory lettuceConnectionFactory = createLettuceConnectionFactory();
        lettuceConnectionFactory.afterPropertiesSet();
        return lettuceConnectionFactory;
    }
    
    
    /*
     LettuceClientConfiguration配置
    */
    public LettuceConnectionFactory createLettuceConnectionFactory() {
        DefaultClientResources.Builder builder = DefaultClientResources.builder();
        DefaultClientResources clientResources = builder.build();
        
        LettuceClientConfiguration clientConfig = getLettuceClientConfiguration(clientResources,
                getProperties().getLettuce().getPool());
        return createLettuceConnectionFactory(clientConfig);
    }
    
    private LettuceClientConfiguration getLettuceClientConfiguration(
            ClientResources clientResources, RedisProperties.Pool pool) {
        LettuceClientConfiguration.LettuceClientConfigurationBuilder builder = createBuilder(pool);
        applyProperties(builder);
        if (StringUtils.hasText(getProperties().getUrl())) {
            ConnectionInfo connectionInfo = parseUrl(getProperties().getUrl());
            if (connectionInfo.isUseSsl()) {
                builder.useSsl();
            }
        }
        builder.clientOptions(createClientOptions());
        builder.clientResources(clientResources);
        return builder.build();
    }
    
    private ClientOptions createClientOptions() {
        ClientOptions.Builder builder = initializeClientOptionsBuilder();
        Duration connectTimeout = getProperties().getConnectTimeout();
        if (connectTimeout != null) {
            builder.socketOptions(SocketOptions.builder().connectTimeout(connectTimeout).build());
        }
        return builder.timeoutOptions(TimeoutOptions.enabled()).build();
    }
    private ClientOptions.Builder initializeClientOptionsBuilder() {
        if (getProperties().getCluster() != null) {
            ClusterClientOptions.Builder builder = ClusterClientOptions.builder();
            org.springframework.boot.autoconfigure.data.redis.RedisProperties.Lettuce.Cluster.Refresh refreshProperties = getProperties().getLettuce().getCluster().getRefresh();
            ClusterTopologyRefreshOptions.Builder refreshBuilder = ClusterTopologyRefreshOptions.builder()
                    .dynamicRefreshSources(refreshProperties.isDynamicRefreshSources());
            if (refreshProperties.getPeriod() != null) {
                refreshBuilder.enablePeriodicRefresh(refreshProperties.getPeriod());
            }
            if (refreshProperties.isAdaptive()) {
                refreshBuilder.enableAllAdaptiveRefreshTriggers();
            }
            return builder.topologyRefreshOptions(refreshBuilder.build());
        }
        return ClientOptions.builder();
    }
    
    private LettuceClientConfiguration.LettuceClientConfigurationBuilder createBuilder(RedisProperties.Pool pool) {
        if (isPoolEnabled(pool)) {
            return new PoolBuilderFactory().createBuilder(pool);
        }
        return LettuceClientConfiguration.builder();
    }
        static class PoolBuilderFactory {

            LettuceClientConfiguration.LettuceClientConfigurationBuilder createBuilder(RedisProperties.Pool properties) {
                return LettucePoolingClientConfiguration.builder().poolConfig(getPoolConfig(properties));
            }

            private GenericObjectPoolConfig<?> getPoolConfig(RedisProperties.Pool properties) {
                GenericObjectPoolConfig<?> config = new GenericObjectPoolConfig<>();
                config.setMaxTotal(properties.getMaxActive());
                config.setMaxIdle(properties.getMaxIdle());
                config.setMinIdle(properties.getMinIdle());
                if (properties.getTimeBetweenEvictionRuns() != null) {
                    config.setTimeBetweenEvictionRuns(properties.getTimeBetweenEvictionRuns());
                }
                if (properties.getMaxWait() != null) {
                    config.setMaxWait(properties.getMaxWait());
                }
                return config;
            }

        }
    
    private LettuceConnectionFactory createLettuceConnectionFactory(LettuceClientConfiguration clientConfiguration) {
        if (getSentinelConfig() != null) {
            return new LettuceConnectionFactory(getSentinelConfig(), clientConfiguration);
        }
        if (getClusterConfiguration() != null) {
            return new LettuceConnectionFactory(getClusterConfiguration(), clientConfiguration);
        }
        return new LettuceConnectionFactory(getStandaloneConfig(), clientConfiguration);
    }
    
    private LettuceClientConfiguration.LettuceClientConfigurationBuilder applyProperties(
            LettuceClientConfiguration.LettuceClientConfigurationBuilder builder) {
        if (getProperties().isSsl()) {
            builder.useSsl();
        }
        if (getProperties().getTimeout() != null) {
            builder.commandTimeout(getProperties().getTimeout());
        }
        if (getProperties().getLettuce() != null) {
            RedisProperties.Lettuce lettuce = getProperties().getLettuce();
            if (lettuce.getShutdownTimeout() != null && !lettuce.getShutdownTimeout().isZero()) {
                builder.shutdownTimeout(getProperties().getLettuce().getShutdownTimeout());
            }
        }
        if (StringUtils.hasText(getProperties().getClientName())) {
            builder.clientName(getProperties().getClientName());
        }
        return builder;
    }
    
    
}
