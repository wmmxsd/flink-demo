package com.wmm.flink.common;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 自定义窗口处理函数，只需要包装窗口信息
 */
public class UrlViewCountResult extends ProcessWindowFunction<Long, UrlViewCount, String, TimeWindow> {

    @Override
    public void process(String url, Context context, Iterable<Long> elements, Collector<UrlViewCount> out) {
        // 结合窗口信息，包装输出内容
        Long start = context.window().getStart();
        Long end = context.window().getEnd();
        // 迭代器中只有一个元素，就是增量聚合函数的计算结果
        out.collect(new UrlViewCount(url, elements.iterator().next(), start, end));
    }
}
