package com.wmm.flink.multiflowconversion.splitstream;

import com.wmm.flink.source.ClickSource;
import com.wmm.flink.transform.Event;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 分流demo
 * 将电商网站收集到的用户行为数据进行一个拆分，根据类型（type）的不同，分为“Mary”的浏览数据、“Bob”的浏览数据等等
 */
public class SplitStreamByFilter {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource());

        // 筛选 Mary 的浏览行为放入 maryStream 流中
        DataStream<Event> maryStream = stream.filter((FilterFunction<Event>) value -> "Mary".equals(value.user));
        // 筛选 Bob 的购买行为放入 bobStream 流中
        DataStream<Event> bobStream = stream.filter((FilterFunction<Event>) value -> "Bob".equals(value.user));
        // 筛选其他人的浏览行为放入 elseStream 流中
        DataStream<Event> elseStream = stream.filter((FilterFunction<Event>) value -> !"Mary".equals(value.user) && !"Bob".equals(value.user));

        maryStream.print("Mary pv");
        bobStream.print("Bob pv");
        elseStream.print("else pv");

        env.execute();
    }
}
