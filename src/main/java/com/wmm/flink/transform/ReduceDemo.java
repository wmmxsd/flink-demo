package com.wmm.flink.transform;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 归约聚合（reduce）demo
 * 统计每个用户访问的频次；进而将所有统计结果分到一组，用另一个 reduce 算子实现 maxBy 的功能，记录所有用户中访问频次最高的那个
 */
public class ReduceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Tom", "./cart", 2000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Tom", "./cart", 2000L)
        );
        //将 Event 数据类型转换成元组类型
        stream.map(value -> Tuple2.of(value.user, 1L), Types.TUPLE(Types.STRING, Types.LONG))
                //使用用户名来分流
                .keyBy(r -> r.f0)
                //每到一条数据，用户 pv 的统计值加 1
                .reduce((value1, value2) -> Tuple2.of(value1.f0, value1.f1 + value2.f1))
                //为每一条数据分配同一个 key，将聚合结果发送到一条流中去
                .keyBy(r -> true)
                //将累加器更新为当前最大的 pv 统计值，然后向下游发送累加器的值
                .reduce((value1, value2) -> value1.f1 > value2.f1 ? value1 : value2)
                .print();
        env.execute();
    }
}
