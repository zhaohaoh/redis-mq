
package com.redismq.common.constant;

/**
 * The enum Transport server type.
 *
 * @author slievrly
 */
public enum TransportServerType {
    /**
     * Native transport server type.
     */
    NATIVE("native"),
    /**
     * Nio transport server type.
     */
    NIO("nio");

    /**
     * The Name.
     */
    public final String name;

    TransportServerType(String name) {
        this.name = name;
    }

    /**
     * Gets type.
     *
     * @param name the name
     * @return the type
     */
    public static TransportServerType getType(String name) {
        for (TransportServerType b : TransportServerType.values()) {
            if (b.name().equalsIgnoreCase(name)) {
                return b;
            }
        }
        throw new IllegalArgumentException("unknown type:" + name);
    }
}
