
package com.redismq.samples.rocket.lock;

/**
 * Used when trying to put message
 */
public interface PutMessageLock {
    void lock();

    void unlock();
}
