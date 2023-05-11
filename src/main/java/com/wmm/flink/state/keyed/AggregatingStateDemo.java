package com.wmm.flink.state.keyed;

import com.wmm.flink.source.ClickSource;
import com.wmm.flink.transform.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

/**
 * 算子状态-聚合状态
 * 对用户点击事件流每 5 个数据统计一次平均时间戳。这是一个类似计数窗口（CountWindow）求平均值的计算，
 * 这里我们可以使用一个有聚合状态的RichFlatMapFunction 来实现
 */
public class AggregatingStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env
                .addSource(new ClickSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Event>forMonotonousTimestamps()
                                .withTimestampAssigner((SerializableTimestampAssigner<Event>) (element, recordTimestamp) -> element.timestamp)
                );

        // 统计每个用户的点击频次，到达 5 次就输出统计结果
        stream
                .keyBy(data -> data.user)
                .flatMap(new RichFlatMapFunction<Event, String>() {
                    // 定义聚合状态，用来计算平均时间戳
                    AggregatingState<Event, Long> avgTsAggState;
                    // 定义一个值状态，用来保存当前用户访问频次
                    ValueState<Long> countState;

                    @Override
                    public void open(Configuration parameters) {
                        avgTsAggState = getRuntimeContext().getAggregatingState(
                                new AggregatingStateDescriptor<>(
                                "avg-ts",
                                new AggregateFunction<Event, Tuple2<Long, Long>, Long>() {
                                    @Override
                                    public Tuple2<Long, Long> createAccumulator() {
                                        return Tuple2.of(0L, 0L);
                                    }

                                    @Override
                                    public Tuple2<Long, Long> add(Event value, Tuple2<Long, Long> accumulator) {
                                        return Tuple2.of(accumulator.f0 + value.timestamp, accumulator.f1 + 1);
                                    }

                                    @Override
                                    public Long getResult(Tuple2<Long, Long> accumulator) {
                                        return accumulator.f0 / accumulator.f1;
                                    }

                                    @Override
                                    public Tuple2<Long, Long> merge(Tuple2<Long, Long> a, Tuple2<Long, Long> b) {
                                        return null;
                                    }
                                },
                                Types.TUPLE(Types.LONG, Types.LONG)
                            )
                        );
                        countState = getRuntimeContext().getState(new ValueStateDescriptor<>("count", Long.class));
                    }

                    @Override
                    public void flatMap(Event value, Collector<String> out) throws Exception {
                        Long count = countState.value();
                        count = count == null ? 1L : count + 1;
                        countState.update(count);
                        avgTsAggState.add(value);
                        // 达到 5 次就输出结果，并清空状态
                        if (count == 5){
                            out.collect(value.user + " 平均时间戳： " + new Timestamp(avgTsAggState.get()));
                            countState.clear();
                        }
                    }
                })
                .print();
        env.execute();
    }
}
