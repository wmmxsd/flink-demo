package com.wmm.flink.multiflowconversion.mergeflow;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 *  合流-窗口同组联结
 */
public class CoGroupDemo {
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
        //Tuple2<String, Long> first, Tuple2<String, Long> second,
        stream1.coGroup(stream2)
                .where(value -> value.f0)
                .equalTo(value -> value.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5L)))
                .apply(
                        (first, second, out) -> out.collect(first + "=>" + second),
                        Types.STRING
                )
                .print();

        env.execute();
    }
}
