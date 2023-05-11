package com.wmm.flink.partition;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * 分区-随机分区
 */
public class ShuffleDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //获取数据源
        List<Tuple3<Integer,Integer,Integer>> data = new ArrayList<>();
        data.add(new Tuple3<>(0,2,2));
        data.add(new Tuple3<>(1,3,3));
        data.add(new Tuple3<>(2,5,6));
        data.add(new Tuple3<>(3,3,5));
        data.add(new Tuple3<>(4,2,9));
        data.add(new Tuple3<>(5,3,8));
        data.add(new Tuple3<>(6,2,7));
        data.add(new Tuple3<>(7,3,9));
        DataStreamSource<Tuple3<Integer,Integer,Integer>> stream = env.fromCollection(data);

        stream.shuffle().print("shuffle").setParallelism(2);
        env.execute();
    }
}
