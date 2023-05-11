package com.wmm.flink.windows;

import com.wmm.flink.source.ClickSource;
import com.wmm.flink.transform.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.HashSet;

/**
 * 窗口聚合demo
 * 统计 10 秒钟的“人均 PV”，每 2 秒统计一次
 */
public class WindowsAggregateFunctionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        SingleOutputStreamOperator<Event> sourceStream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(1L))
                                .withTimestampAssigner((element, recordTimestamp) -> element.timestamp)
                );

        sourceStream.keyBy(value -> true)
                .window(SlidingEventTimeWindows.of(Time.seconds(10L), Time.seconds(2L)))
                .aggregate(getAggregateFunction())
                .print();

        env.execute();
    }

    private static AggregateFunction<Event, Tuple2<HashSet<String>, Long>, Double> getAggregateFunction() {
        return new AggregateFunction<Event, Tuple2<HashSet<String>, Long>, Double>() {
            @Override
            public Tuple2<HashSet<String>, Long> createAccumulator() {
                return Tuple2.of(new HashSet<>(), 0L);
            }

            @Override
            public Tuple2<HashSet<String>, Long> add(Event value, Tuple2<HashSet<String>, Long> accumulator) {
                accumulator.f0.add(value.user);
                return Tuple2.of(accumulator.f0, accumulator.f1 + 1L);
            }

            @Override
            public Double getResult(Tuple2<HashSet<String>, Long> accumulator) {
                return (double) (accumulator.f1 / accumulator.f0.size());
            }

            @Override
            public Tuple2<HashSet<String>, Long> merge(Tuple2<HashSet<String>, Long> a, Tuple2<HashSet<String>, Long> b) {
                return null;
            }
        };
    }
}
