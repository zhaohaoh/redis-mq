
package com.redismq.rebalance;

import java.util.List;


public interface AllocateMessageQueueStrategy {


    List<String> allocate(
        final String currentCID,
        final List<String> mqNames,
        final List<String> cidNames
    );

    /**
     * Algorithm name
     *
     * @return The strategy name
     */
    String getName();
}
