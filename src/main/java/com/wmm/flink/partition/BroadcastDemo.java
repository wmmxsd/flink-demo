package com.wmm.flink.partition;

import com.wmm.flink.transform.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 分区-广播
 */
public class BroadcastDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L)
        );
        // 经广播后打印输出，并行度为 4
        stream.broadcast().print("broadcast").setParallelism(4);
        env.execute();
    }
}
