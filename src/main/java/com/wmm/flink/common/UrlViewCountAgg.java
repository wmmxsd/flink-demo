package com.wmm.flink.common;

import com.wmm.flink.transform.Event;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * 自定义增量聚合函数，来一条数据就加一
 */
public class UrlViewCountAgg implements AggregateFunction<Event, Long, Long> {
    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(Event value, Long accumulator) {
        return accumulator + 1;
    }

    @Override
    public Long getResult(Long accumulator) {
        return accumulator;
    }

    @Override
    public Long merge(Long a, Long b) {
        return null;
    }
}
