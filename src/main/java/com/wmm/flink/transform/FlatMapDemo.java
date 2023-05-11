package com.wmm.flink.transform;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.List;

/**
 * flatMap转换算子demo
 */
public class FlatMapDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<List<Event>> stream = env.fromElements(
                Arrays.asList(new Event("Mary", "./home", 1000L),
                        new Event("Bob", "./cart", 2000L)),
                Arrays.asList(new Event("XiaoMing", "./home", 1000L),
                        new Event("XiaoHong", "./cart", 2000L))
        );
        //stream.print();
        stream.flatMap(new MyFlatMap()).print();
        env.execute();
    }

    public static class MyFlatMap implements FlatMapFunction<List<Event>, Event> {
        @Override
        public void flatMap(List<Event> value, Collector<Event> out) {
            value.forEach(out::collect);
        }
    }
}