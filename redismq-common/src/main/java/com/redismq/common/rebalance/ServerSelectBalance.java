package com.redismq.common.rebalance;

import java.util.List;

public interface ServerSelectBalance {
   
    <T> T select(List<T> invokers, String id) throws Exception;
}
