package com.redismq.common.exception;

public class RedisMqException extends RuntimeException {
    public RedisMqException() {
        super();
    }

    public RedisMqException(String message) {
        super(message);
    }

    public RedisMqException(String message, Throwable cause) {
        super(message, cause);
    }
}
