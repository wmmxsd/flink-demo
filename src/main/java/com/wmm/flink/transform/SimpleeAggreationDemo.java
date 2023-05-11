package com.wmm.flink.transform;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * 简单聚合demo
 */
public class SimpleeAggreationDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //获取数据源
        List<Tuple3<Integer,Integer,Integer>> data = new ArrayList<>();
        data.add(new Tuple3<>(0,2,2));
        data.add(new Tuple3<>(0,3,3));
        data.add(new Tuple3<>(0,5,6));
        data.add(new Tuple3<>(0,3,5));
        data.add(new Tuple3<>(2,2,9));
        data.add(new Tuple3<>(2,3,8));
        data.add(new Tuple3<>(2,2,7));
        data.add(new Tuple3<>(2,3,9));

        DataStreamSource<Tuple3<Integer,Integer,Integer>> stream = env.fromCollection(data);

        //对二元组的第2个元素进行累加聚合
        stream.keyBy(r -> r.f0).sum(2).print();
        stream.keyBy(r -> r.f0).sum("f2").print();
        //对二元组的第2个元素进行求最大值聚合
        stream.keyBy(r -> r.f0).max(2).print();
        stream.keyBy(r -> r.f0).max("f2").print();
        //对二元组的第2个元素进行求最小值聚合
        stream.keyBy(r -> r.f0).min(2).print();
        stream.keyBy(r -> r.f0).min("f2").print();
        //对二元组的第2个元素进行求最大值聚合
        stream.keyBy(r -> r.f0).maxBy(2).print();
        stream.keyBy(r -> r.f0).maxBy("f2").print();
        //对二元组的第2个元素进行求最小值聚合
        stream.keyBy(r -> r.f0).minBy(2).print();
        stream.keyBy(r -> r.f0).minBy("f2").print();

        env.execute();
    }
}