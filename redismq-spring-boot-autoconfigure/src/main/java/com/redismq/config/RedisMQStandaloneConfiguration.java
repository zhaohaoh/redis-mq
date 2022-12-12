
package com.redismq.config;

import org.springframework.data.redis.connection.RedisPassword;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

public class RedisMQStandaloneConfiguration extends RedisStandaloneConfiguration {

	private static final String DEFAULT_HOST = "localhost";
	private static final int DEFAULT_PORT = 6379;

	private String hostName = DEFAULT_HOST;
	private int port = DEFAULT_PORT;
	private int database;
	private String username = null;
	private RedisPassword password = RedisPassword.none();

	/**
	 * Create a new default {@link RedisMQStandaloneConfiguration}.
	 */
	public RedisMQStandaloneConfiguration() {}

	/**
	 * Create a new {@link RedisMQStandaloneConfiguration} given {@code hostName}.
	 *
	 * @param hostName must not be {@literal null} or empty.
	 */
	public RedisMQStandaloneConfiguration(String hostName) {
		this(hostName, DEFAULT_PORT);
	}

	/**
	 * Create a new {@link RedisMQStandaloneConfiguration} given {@code hostName} and {@code port}.
	 *
	 * @param hostName must not be {@literal null} or empty.
	 * @param port a valid TCP port (1-65535).
	 */
	public RedisMQStandaloneConfiguration(String hostName, int port) {

		Assert.hasText(hostName, "Host name must not be null or empty!");
		Assert.isTrue(port >= 1 && port <= 65535,
				() -> String.format("Port %d must be a valid TCP port in the range between 1-65535!", port));

		this.hostName = hostName;
		this.port = port;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisConfiguration.WithHostAndPort#getHostName()
	 */
	@Override
	public String getHostName() {
		return hostName;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisConfiguration.WithHostAndPort#getPort()
	 */
	@Override
	public int getPort() {
		return port;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisConfiguration.WithHostAndPort#setHostName(java.lang.String)
	 */
	@Override
	public void setHostName(String hostName) {
		this.hostName = hostName;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisConfiguration.WithHostAndPort#setPort(int)
	 */
	public void setPort(int port) {
		this.port = port;
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
		if (!(o instanceof RedisMQStandaloneConfiguration)) {
			return false;
		}
		RedisMQStandaloneConfiguration that = (RedisMQStandaloneConfiguration) o;
		if (port != that.port) {
			return false;
		}
		if (database != that.database) {
			return false;
		}
		if (!ObjectUtils.nullSafeEquals(hostName, that.hostName)) {
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
		int result = ObjectUtils.nullSafeHashCode(hostName);
		result = 31 * result + port;
		result = 31 * result + database;
		result = 31 * result + ObjectUtils.nullSafeHashCode(username);
		result = 31 * result + ObjectUtils.nullSafeHashCode(password);
		return result;
	}
}
