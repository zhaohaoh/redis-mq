package com.redismq.common.rebalance;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * 随机
 *
 * @author hzh
 * @date 2024/05/05
 */
public class RandomBalance implements ServerSelectBalance {
    
    @Override
    public <T> T select(List<T> invokers, String id) {
        int length = invokers.size();
        return invokers.get(ThreadLocalRandom.current().nextInt(length));
    }
}
