package com.wmm.flink.multiflowconversion.mergeflow;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * 合流-窗口联结
 * 在电商网站中，往往需要统计用户不同行为之间的转化，这就需要对不同的行为数据流，
 * 按照用户 ID 进行分组后再合并，以分析它们之间的关联。如果这些是以固定时间周期（比如
 * 1 小时）来统计的，那我们就可以使用窗口 join 来实现这样的需求。
 */
public class WindowJoinDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<Tuple2<String, Long>> stream1 = env.fromElements(
                        Tuple2.of("a", 1000L),
                        Tuple2.of("b", 1000L),
                        Tuple2.of("a", 2000L),
                        Tuple2.of("b", 2000L),
                        Tuple2.of("c", 2000L)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple2<String, Long>>forMonotonousTimestamps()
                                .withTimestampAssigner(
                                        (SerializableTimestampAssigner<Tuple2<String, Long>>) (stringLongTuple2, l) -> stringLongTuple2.f1
                                )
                );
        DataStream<Tuple2<String, Long>> stream2 = env.fromElements(
                        Tuple2.of("a", 3000L),
                        Tuple2.of("b", 3000L),
                        Tuple2.of("a", 4000L),
                        Tuple2.of("b", 4000L)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple2<String, Long>>forMonotonousTimestamps()
                                .withTimestampAssigner(
                                    (SerializableTimestampAssigner<Tuple2<String, Long>>) (stringLongTuple2, l) -> stringLongTuple2.f1
                                )
                );

        stream1.join(stream2)
                .where(value -> value.f0)
                .equalTo(value -> value.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5L)))
                .apply((JoinFunction<Tuple2<String, Long>, Tuple2<String, Long>, String>) (first, second) -> first + "=>" + second)
                .print();

        env.execute();
    }
}
