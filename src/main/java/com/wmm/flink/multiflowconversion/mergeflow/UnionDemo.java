package com.wmm.flink.multiflowconversion.mergeflow;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 合流-联合操作
 */
public class UnionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Integer> source0 = env.fromElements(1, 2, 3, 4, 5);
        DataStreamSource<Integer> source1 = env.fromElements(6, 7, 8, 9, 10);

        source0.union(source1).print();

        env.execute();
    }
}
