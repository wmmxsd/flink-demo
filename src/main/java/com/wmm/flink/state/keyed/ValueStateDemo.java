package com.wmm.flink.state.keyed;

import com.wmm.flink.source.ClickSource;
import com.wmm.flink.transform.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 算子状态-值状态
 * 定时统计每个用户的 pv 数据
 */
public class ValueStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> dataStreamSource = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forMonotonousTimestamps()
                                .withTimestampAssigner((element, recordTimestamp) -> element.timestamp)
                );

        dataStreamSource.print("input");

        dataStreamSource.keyBy(value -> value.user)
                .process(new KeyedProcessFunction<String, Event, String>() {
                    /**
                     * 当前 pv 值
                     */
                    ValueState<Long> countState;
                    /**
                     * 定时器时间戳
                     */
                    ValueState<Long> timerTsState;

                    @Override
                    public void open(Configuration parameters) {
                        //在open生命周期方法中通过运行时上下文获取状态。
                        countState = getRuntimeContext().getState(new ValueStateDescriptor<>("count", Long.class));
                        timerTsState = getRuntimeContext().getState(new ValueStateDescriptor<>("timerTs", Long.class));
                    }

                    @Override
                    public void processElement(Event value, KeyedProcessFunction<String, Event, String>.Context ctx, Collector<String> out) throws Exception {
                        // 更新 count 值
                        countState.update(countState.value() == null ? 1L : countState.value() + 1);

                        if (timerTsState.value() == null) {
                            ctx.timerService().registerEventTimeTimer(value.timestamp + 10 * 1000L);
                            timerTsState.update(value.timestamp + 10 * 1000L);
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<String, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                        out.collect(ctx.getCurrentKey() + " pv: " + countState.value());
                    }
                })
                .print();

        env.execute();
    }
}
