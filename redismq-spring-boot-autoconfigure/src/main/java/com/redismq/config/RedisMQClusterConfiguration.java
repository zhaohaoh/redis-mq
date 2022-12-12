package com.redismq.config;

import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.PropertySource;
import org.springframework.data.redis.connection.RedisClusterConfiguration;
import org.springframework.data.redis.connection.RedisNode;
import org.springframework.data.redis.connection.RedisPassword;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.NumberUtils;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

import java.util.*;

import static org.springframework.util.StringUtils.commaDelimitedListToSet;
import static org.springframework.util.StringUtils.split;


public class RedisMQClusterConfiguration extends RedisClusterConfiguration {

	private static final String REDIS_CLUSTER_NODES_CONFIG_PROPERTY = "spring.redismq.client.cluster.nodes";
	private static final String REDIS_CLUSTER_MAX_REDIRECTS_CONFIG_PROPERTY = "spring.redismq.client.cluster.max-redirects";

	private Set<RedisNode> clusterNodes;
	private @Nullable Integer maxRedirects;
	private @Nullable String username = null;
	private RedisPassword password = RedisPassword.none();

	/**
	 * Creates new {@link RedisMQClusterConfiguration}.
	 */
	public RedisMQClusterConfiguration() {
		this(new MapPropertySource("RedisClusterConfiguration", Collections.emptyMap()));
	}

	/**
	 * Creates {@link RedisMQClusterConfiguration} for given hostPort combinations.
	 *
	 * <pre>
	 * <code>
	 * clusterHostAndPorts[0] = 127.0.0.1:23679
	 * clusterHostAndPorts[1] = 127.0.0.1:23680 ...
	 * </code>
	 * </pre>
	 *
	 * @param clusterNodes must not be {@literal null}.
	 */
	public RedisMQClusterConfiguration(Collection<String> clusterNodes) {
		this(new MapPropertySource("RedisClusterConfiguration", asMap(clusterNodes, -1)));
	}

	/**
	 * Creates {@link RedisMQClusterConfiguration} looking up values in given {@link PropertySource}.
	 *
	 * <pre>
	 * <code>
	 * spring.redis.cluster.nodes=127.0.0.1:23679,127.0.0.1:23680,127.0.0.1:23681
	 * spring.redis.cluster.max-redirects=3
	 * </code>
	 * </pre>
	 *
	 * @param propertySource must not be {@literal null}.
	 */
	public RedisMQClusterConfiguration(PropertySource<?> propertySource) {

		Assert.notNull(propertySource, "PropertySource must not be null!");

		this.clusterNodes = new LinkedHashSet<>();

		if (propertySource.containsProperty(REDIS_CLUSTER_NODES_CONFIG_PROPERTY)) {
			appendClusterNodes(
					commaDelimitedListToSet(propertySource.getProperty(REDIS_CLUSTER_NODES_CONFIG_PROPERTY).toString()));
		}
		if (propertySource.containsProperty(REDIS_CLUSTER_MAX_REDIRECTS_CONFIG_PROPERTY)) {
			this.maxRedirects = NumberUtils.parseNumber(
					propertySource.getProperty(REDIS_CLUSTER_MAX_REDIRECTS_CONFIG_PROPERTY).toString(), Integer.class);
		}
	}

	/**
	 * Set {@literal cluster nodes} to connect to.
	 *
	 * @param nodes must not be {@literal null}.
	 */
	public void setClusterNodes(Iterable<RedisNode> nodes) {

		Assert.notNull(nodes, "Cannot set cluster nodes to 'null'.");

		this.clusterNodes.clear();

		for (RedisNode clusterNode : nodes) {
			addClusterNode(clusterNode);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisConfiguration.ClusterConfiguration#getClusterNodes()
	 */
	@Override
	public Set<RedisNode> getClusterNodes() {
		return Collections.unmodifiableSet(clusterNodes);
	}

	/**
	 * Add a cluster node to configuration.
	 *
	 * @param node must not be {@literal null}.
	 */
	public void addClusterNode(RedisNode node) {

		Assert.notNull(node, "ClusterNode must not be 'null'.");
		this.clusterNodes.add(node);
	}

	/**
	 * @return this.
	 */
	public RedisMQClusterConfiguration clusterNode(RedisNode node) {

		this.clusterNodes.add(node);
		return this;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisConfiguration.ClusterConfiguration#getMaxRedirects()
	 */
	@Override
	public Integer getMaxRedirects() {
		return maxRedirects != null && maxRedirects > Integer.MIN_VALUE ? maxRedirects : null;
	}

	/**
	 * @param maxRedirects the max number of redirects to follow.
	 */
	public void setMaxRedirects(int maxRedirects) {

		Assert.isTrue(maxRedirects >= 0, "MaxRedirects must be greater or equal to 0");
		this.maxRedirects = maxRedirects;
	}

	/**
	 * @param host Redis cluster node host name or ip address.
	 * @param port Redis cluster node port.
	 * @return this.
	 */
	public RedisMQClusterConfiguration clusterNode(String host, Integer port) {
		return clusterNode(new RedisNode(host, port));
	}

	private void appendClusterNodes(Set<String> hostAndPorts) {

		for (String hostAndPort : hostAndPorts) {
			addClusterNode(readHostAndPortFromString(hostAndPort));
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisConfiguration.WithAuthentication#setUsername(String)
	 */
	@Override
	public void setUsername(@Nullable String username) {
		this.username = username;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisConfiguration.WithAuthentication#getUsername()
	 */
	@Nullable
	@Override
	public String getUsername() {
		return this.username;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisConfiguration.WithPassword#getPassword()
	 */
	@Override
	public RedisPassword getPassword() {
		return password;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisConfiguration.WithPassword#setPassword(org.springframework.data.redis.connection.RedisPassword)
	 */
	@Override
	public void setPassword(RedisPassword password) {

		Assert.notNull(password, "RedisPassword must not be null!");

		this.password = password;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (!(o instanceof RedisMQClusterConfiguration)) {
			return false;
		}
		RedisMQClusterConfiguration that = (RedisMQClusterConfiguration) o;
		if (!ObjectUtils.nullSafeEquals(clusterNodes, that.clusterNodes)) {
			return false;
		}
		if (!ObjectUtils.nullSafeEquals(maxRedirects, that.maxRedirects)) {
			return false;
		}
		if (!ObjectUtils.nullSafeEquals(username, that.username)) {
			return false;
		}
		return ObjectUtils.nullSafeEquals(password, that.password);
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		int result = ObjectUtils.nullSafeHashCode(clusterNodes);
		result = 31 * result + ObjectUtils.nullSafeHashCode(maxRedirects);
		result = 31 * result + ObjectUtils.nullSafeHashCode(username);
		result = 31 * result + ObjectUtils.nullSafeHashCode(password);
		return result;
	}

	private RedisNode readHostAndPortFromString(String hostAndPort) {

		String[] args = split(hostAndPort, ":");

		Assert.notNull(args, "HostAndPort need to be seperated by  ':'.");
		Assert.isTrue(args.length == 2, "Host and Port String needs to specified as host:port");
		return new RedisNode(args[0], Integer.valueOf(args[1]));
	}

	/**
	 * @param clusterHostAndPorts must not be {@literal null} or empty.
	 * @param redirects the max number of redirects to follow.
	 * @return cluster config map with properties.
	 */
	private static Map<String, Object> asMap(Collection<String> clusterHostAndPorts, int redirects) {

		Assert.notNull(clusterHostAndPorts, "ClusterHostAndPorts must not be null!");
		Assert.noNullElements(clusterHostAndPorts, "ClusterHostAndPorts must not contain null elements!");

		Map<String, Object> map = new HashMap<>();
		map.put(REDIS_CLUSTER_NODES_CONFIG_PROPERTY, StringUtils.collectionToCommaDelimitedString(clusterHostAndPorts));

		if (redirects >= 0) {
			map.put(REDIS_CLUSTER_MAX_REDIRECTS_CONFIG_PROPERTY, redirects);
		}

		return map;
	}
}
