package com.redismq.common.exception;

public class RedisMQRpcException extends RuntimeException{
    public RedisMQRpcException() {
        super();
    }
    
    public RedisMQRpcException(String message) {
        super(message);
    }
    
    public RedisMQRpcException(String message, Throwable cause) {
        super(message, cause);
    }
}
