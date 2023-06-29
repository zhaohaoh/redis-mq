package com.redismq.samples.rocket.config;

import lombok.Getter;

@Getter
public enum PutMessageStatus {
    PUT_OK,
    FLUSH_DISK_TIMEOUT,
    FLUSH_SLAVE_TIMEOUT,
    SLAVE_NOT_AVAILABLE,
    SERVICE_NOT_AVAILABLE,
    CREATE_MAPEDFILE_FAILED,
    MESSAGE_ILLEGAL,
    PROPERTIES_SIZE_EXCEEDED,
    OS_PAGECACHE_BUSY,
    UNKNOWN_ERROR,
}
