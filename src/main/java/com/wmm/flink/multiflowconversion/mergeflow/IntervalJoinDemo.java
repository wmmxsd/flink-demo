package com.wmm.flink.multiflowconversion.mergeflow;

import com.wmm.flink.transform.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * 合流-间隔联接
 * 在电商网站中，某些用户行为往往会有短时间内的强关联。我们这里举一个例子，我们有
 * 两条流，一条是下订单的流，一条是浏览数据的流。我们可以针对同一个用户，来做这样一个
 * 联结。也就是使用一个用户的下订单的事件和这个用户的最近十分钟的浏览数据进行一个联结
 * 查询。
 */
public class IntervalJoinDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Tuple3<String, String, Long>> orderStream = env.fromElements(
                        Tuple3.of("Mary", "order-1", 5000L),
                        Tuple3.of("Alice", "order-2", 5000L),
                        Tuple3.of("Bob", "order-3", 20000L),
                        Tuple3.of("Alice", "order-4", 20000L),
                        Tuple3.of("Cary", "order-5", 51000L)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple3<String, String, Long>>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                (SerializableTimestampAssigner<Tuple3<String, String, Long>>) (element, recordTimestamp) -> element.f2
                        )
                );
        SingleOutputStreamOperator<Event> clickStream = env.fromElements(
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=100", 3000L),
                new Event("Alice", "./prod?id=200", 3500L),
                new Event("Bob", "./prod?id=2", 2500L),
                new Event("Alice", "./prod?id=300", 36000L),
                new Event("Bob", "./home", 30000L),
                new Event("Bob", "./prod?id=1", 23000L),
                new Event("Bob", "./prod?id=3", 33000L)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forMonotonousTimestamps()
                                .withTimestampAssigner(
                                        (SerializableTimestampAssigner<Event>) (element, recordTimestamp) -> element.timestamp
                                )
        );

        orderStream.keyBy(value -> value.f0)
                .intervalJoin(clickStream.keyBy(value -> value.user))
                .between(Time.seconds(-5), Time.seconds(10))
                .process(new ProcessJoinFunction<Tuple3<String, String, Long>, Event,String>() {
                    @Override
                    public void processElement(Tuple3<String, String, Long> left, Event right, ProcessJoinFunction<Tuple3<String, String, Long>, Event, String>.Context ctx, Collector<String> out) throws Exception {
                        out.collect(right + " => " + left);
                    }
                })
                .print();

                env.execute();
    }
}
