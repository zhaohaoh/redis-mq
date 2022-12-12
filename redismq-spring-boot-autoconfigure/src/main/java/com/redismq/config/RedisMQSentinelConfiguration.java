
package com.redismq.config;

import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.PropertySource;
import org.springframework.data.redis.connection.NamedNode;
import org.springframework.data.redis.connection.RedisNode;
import org.springframework.data.redis.connection.RedisPassword;
import org.springframework.data.redis.connection.RedisSentinelConfiguration;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

import java.util.*;

import static org.springframework.util.StringUtils.commaDelimitedListToSet;
import static org.springframework.util.StringUtils.split;

public class RedisMQSentinelConfiguration extends RedisSentinelConfiguration {

	private static final String REDIS_SENTINEL_MASTER_CONFIG_PROPERTY = "spring.redismq.client.sentinel.master";
	private static final String REDIS_SENTINEL_NODES_CONFIG_PROPERTY = "spring.redismq.client.sentinel.nodes";
	private static final String REDIS_SENTINEL_PASSWORD_CONFIG_PROPERTY = "spring.redismq.client.sentinel.password";

	private @Nullable
	NamedNode master;
	private Set<RedisNode> sentinels;
	private int database;

	private @Nullable String dataNodeUsername = null;
	private RedisPassword dataNodePassword = RedisPassword.none();
	private RedisPassword sentinelPassword = RedisPassword.none();

	/**
	 * Creates new {@link RedisMQSentinelConfiguration}.
	 */
	public RedisMQSentinelConfiguration() {
		this(new MapPropertySource("RedisSentinelConfiguration", Collections.emptyMap()));
	}

	/**
	 * Creates {@link RedisMQSentinelConfiguration} for given hostPort combinations.
	 *
	 * <pre>
	 * sentinelHostAndPorts[0] = 127.0.0.1:23679 sentinelHostAndPorts[1] = 127.0.0.1:23680 ...
	 * </pre>
	 *
	 * @param sentinelHostAndPorts must not be {@literal null}.
	 * @since 1.5
	 */
	public RedisMQSentinelConfiguration(String master, Set<String> sentinelHostAndPorts) {
		this(new MapPropertySource("RedisSentinelConfiguration", asMap(master, sentinelHostAndPorts)));
	}

	/**
	 * Creates {@link RedisMQSentinelConfiguration} looking up values in given {@link PropertySource}.
	 *
	 * <pre>
	 * <code>
	 * spring.redis.sentinel.master=myMaster
	 * spring.redis.sentinel.nodes=127.0.0.1:23679,127.0.0.1:23680,127.0.0.1:23681
	 * </code>
	 * </pre>
	 *
	 * @param propertySource must not be {@literal null}.
	 * @since 1.5
	 */
	public RedisMQSentinelConfiguration(PropertySource<?> propertySource) {

		Assert.notNull(propertySource, "PropertySource must not be null!");

		this.sentinels = new LinkedHashSet<>();

		if (propertySource.containsProperty(REDIS_SENTINEL_MASTER_CONFIG_PROPERTY)) {
			this.setMaster(propertySource.getProperty(REDIS_SENTINEL_MASTER_CONFIG_PROPERTY).toString());
		}

		if (propertySource.containsProperty(REDIS_SENTINEL_NODES_CONFIG_PROPERTY)) {
			appendSentinels(
					commaDelimitedListToSet(propertySource.getProperty(REDIS_SENTINEL_NODES_CONFIG_PROPERTY).toString()));
		}

		if (propertySource.containsProperty(REDIS_SENTINEL_PASSWORD_CONFIG_PROPERTY)) {
			this.setSentinelPassword(propertySource.getProperty(REDIS_SENTINEL_PASSWORD_CONFIG_PROPERTY).toString());
		}
	}

	/**
	 * Set {@literal Sentinels} to connect to.
	 *
	 * @param sentinels must not be {@literal null}.
	 */
	public void setSentinels(Iterable<RedisNode> sentinels) {

		Assert.notNull(sentinels, "Cannot set sentinels to 'null'.");

		this.sentinels.clear();

		for (RedisNode sentinel : sentinels) {
			addSentinel(sentinel);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisConfiguration.SentinelConfiguration#getSentinels()
	 */
	public Set<RedisNode> getSentinels() {
		return Collections.unmodifiableSet(sentinels);
	}

	/**
	 * Add sentinel.
	 *
	 * @param sentinel must not be {@literal null}.
	 */
	public void addSentinel(RedisNode sentinel) {

		Assert.notNull(sentinel, "Sentinel must not be 'null'.");
		this.sentinels.add(sentinel);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisConfiguration.SentinelConfiguration#setMaster(org.springframework.data.redis.connection.NamedNode)
	 */
	public void setMaster(NamedNode master) {

		Assert.notNull(master, "Sentinel master node must not be 'null'.");
		this.master = master;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisConfiguration.SentinelConfiguration#getMaster()
	 */
	public NamedNode getMaster() {
		return master;
	}

	/**
	 * @see #setMaster(String)
	 * @param master The master node name.
	 * @return this.
	 */
	public RedisMQSentinelConfiguration master(String master) {
		this.setMaster(master);
		return this;
	}

	/**
	 * @see #setMaster(NamedNode)
	 * @param master the master node
	 * @return this.
	 */
	public RedisMQSentinelConfiguration master(NamedNode master) {
		this.setMaster(master);
		return this;
	}

	/**
	 * @see #addSentinel(RedisNode)
	 * @param sentinel the node to add as sentinel.
	 * @return this.
	 */
	public RedisMQSentinelConfiguration sentinel(RedisNode sentinel) {
		this.addSentinel(sentinel);
		return this;
	}

	/**
	 * @see #sentinel(RedisNode)
	 * @param host redis sentinel node host name or ip.
	 * @param port redis sentinel port.
	 * @return this.
	 */
	public RedisMQSentinelConfiguration sentinel(String host, Integer port) {
		return sentinel(new RedisNode(host, port));
	}

	private void appendSentinels(Set<String> hostAndPorts) {

		for (String hostAndPort : hostAndPorts) {
			addSentinel(readHostAndPortFromString(hostAndPort));
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisConfiguration.WithDatabaseIndex#getDatabase()
	 */
	@Override
	public int getDatabase() {
		return database;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisConfiguration.WithDatabaseIndex#setDatabase(int)
	 */
	@Override
	public void setDatabase(int index) {

		Assert.isTrue(index >= 0, () -> String.format("Invalid DB index '%s' (a positive index required)", index));

		this.database = index;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisConfiguration.WithAuthentication#setUsername(String)
	 */
	@Override
	public void setUsername(@Nullable String username) {
		this.dataNodeUsername = username;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisConfiguration.WithAuthentication#getUsername()
	 */
	@Nullable
	@Override
	public String getUsername() {
		return this.dataNodeUsername;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisConfiguration.WithPassword#getPassword()
	 */
	@Override
	public RedisPassword getPassword() {
		return dataNodePassword;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisConfiguration.WithPassword#setPassword(org.springframework.data.redis.connection.RedisPassword)
	 */
	@Override
	public void setPassword(RedisPassword password) {

		Assert.notNull(password, "RedisPassword must not be null!");

		this.dataNodePassword = password;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisConfiguration.SentinelConfiguration#setSentinelPassword(org.springframework.data.redis.connection.RedisPassword)
	 */
	public void setSentinelPassword(RedisPassword sentinelPassword) {

		Assert.notNull(sentinelPassword, "SentinelPassword must not be null!");
		this.sentinelPassword = sentinelPassword;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisConfiguration.SentinelConfiguration#setSentinelPassword()
	 */
	@Override
	public RedisPassword getSentinelPassword() {
		return sentinelPassword;
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
		if (!(o instanceof RedisMQSentinelConfiguration)) {
			return false;
		}
		RedisMQSentinelConfiguration that = (RedisMQSentinelConfiguration) o;
		if (database != that.database) {
			return false;
		}
		if (!ObjectUtils.nullSafeEquals(master, that.master)) {
			return false;
		}
		if (!ObjectUtils.nullSafeEquals(sentinels, that.sentinels)) {
			return false;
		}
		if (!ObjectUtils.nullSafeEquals(dataNodeUsername, that.dataNodeUsername)) {
			return false;
		}
		if (!ObjectUtils.nullSafeEquals(dataNodePassword, that.dataNodePassword)) {
			return false;
		}
		return ObjectUtils.nullSafeEquals(sentinelPassword, that.sentinelPassword);
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		int result = ObjectUtils.nullSafeHashCode(master);
		result = 31 * result + ObjectUtils.nullSafeHashCode(sentinels);
		result = 31 * result + database;
		result = 31 * result + ObjectUtils.nullSafeHashCode(dataNodeUsername);
		result = 31 * result + ObjectUtils.nullSafeHashCode(dataNodePassword);
		result = 31 * result + ObjectUtils.nullSafeHashCode(sentinelPassword);
		return result;
	}

	private RedisNode readHostAndPortFromString(String hostAndPort) {

		String[] args = split(hostAndPort, ":");

		Assert.notNull(args, "HostAndPort need to be seperated by  ':'.");
		Assert.isTrue(args.length == 2, "Host and Port String needs to specified as host:port");
		return new RedisNode(args[0], Integer.valueOf(args[1]).intValue());
	}

	/**
	 * @param master must not be {@literal null} or empty.
	 * @param sentinelHostAndPorts must not be {@literal null}.
	 * @return configuration map.
	 */
	private static Map<String, Object> asMap(String master, Set<String> sentinelHostAndPorts) {

		Assert.hasText(master, "Master address must not be null or empty!");
		Assert.notNull(sentinelHostAndPorts, "SentinelHostAndPorts must not be null!");
		Assert.noNullElements(sentinelHostAndPorts, "ClusterHostAndPorts must not contain null elements!");

		Map<String, Object> map = new HashMap<>();
		map.put(REDIS_SENTINEL_MASTER_CONFIG_PROPERTY, master);
		map.put(REDIS_SENTINEL_NODES_CONFIG_PROPERTY, StringUtils.collectionToCommaDelimitedString(sentinelHostAndPorts));

		return map;
	}
}
