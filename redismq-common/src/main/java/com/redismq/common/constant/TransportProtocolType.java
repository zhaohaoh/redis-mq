package com.redismq.common.constant;


public enum TransportProtocolType {
    /**
     * Tcp transport protocol type.
     */
    TCP("tcp"),

    /**
     * Unix domain socket transport protocol type.
     */
    UNIX_DOMAIN_SOCKET("unix-domain-socket");

    /**
     * The Name.
     */
    public final String name;

    TransportProtocolType(String name) {
        this.name = name;
    }

    /**
     * Gets type.
     *
     * @param name the name
     * @return the type
     */
    public static TransportProtocolType getType(String name) {
        name = name.trim().replace('-', '_');
        for (TransportProtocolType b : TransportProtocolType.values()) {
            if (b.name().equalsIgnoreCase(name)) {
                return b;
            }
        }
        throw new IllegalArgumentException("unknown type:" + name);
    }
}
