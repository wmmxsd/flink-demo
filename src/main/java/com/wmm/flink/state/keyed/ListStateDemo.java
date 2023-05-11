package com.wmm.flink.state.keyed;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 算子状态-列表状态
 * 实现一下这个 SQL语句 “SELECT * FROM A INNER JOIN B WHERE A.id = B.id；”的功能
 */
public class ListStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Tuple3<String, String, Long>> stream1 =
                env
                        .fromElements(
                                Tuple3.of("a", "stream-1", 1000L),
                                Tuple3.of("b", "stream-1", 2000L)
                        )
                        .assignTimestampsAndWatermarks(
                                WatermarkStrategy
                                        .<Tuple3<String, String, Long>>forMonotonousTimestamps()
                                        .withTimestampAssigner((SerializableTimestampAssigner<Tuple3<String, String, Long>>) (t, l) -> t.f2)
                        );

        SingleOutputStreamOperator<Tuple3<String, String, Long>> stream2 = env
                .fromElements(
                        Tuple3.of("a", "stream-2", 3000L),
                        Tuple3.of("b", "stream-2", 4000L)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple3<String, String, Long>>forMonotonousTimestamps()
                                .withTimestampAssigner((SerializableTimestampAssigner<Tuple3<String, String, Long>>) (t, l) -> t.f2)
                );

        stream1
                .keyBy(value -> value.f0)
                .connect(stream2.keyBy(value -> value.f0))
                .process(new CoProcessFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, String>() {
                    /**
                     * stream1的列表状态
                     */
                    private ListState<Tuple3<String, String, Long>> stream1ListState;
                    /**
                     *stream2的列表状态
                     */
                    private ListState<Tuple3<String, String, Long>> stream2ListState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        stream1ListState = getRuntimeContext().getListState(
                                new ListStateDescriptor<>("stream1-list", Types.TUPLE(Types.STRING, Types.STRING))
                        );
                        stream2ListState = getRuntimeContext().getListState(
                                new ListStateDescriptor<>("stream2-list", Types.TUPLE(Types.STRING, Types.STRING))
                        );
                    }

                    @Override
                    public void processElement1(Tuple3<String, String, Long> value, CoProcessFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, String>.Context ctx, Collector<String> out) throws Exception {
                        stream1ListState.add(value);
                        for (Tuple3<String, String, Long> right : stream2ListState.get()) {
                            out.collect(value + " => " + right);
                        }
                    }

                    @Override
                    public void processElement2(Tuple3<String, String, Long> value, CoProcessFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, String>.Context ctx, Collector<String> out) throws Exception {
                        stream2ListState.add(value);
                        for (Tuple3<String, String, Long> left : stream1ListState.get()) {
                            out.collect(left + " => " + value);
                        }
                    }
                })
                .print();


        env.execute();
    }
}
