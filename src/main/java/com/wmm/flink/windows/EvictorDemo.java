package com.wmm.flink.windows;

import com.wmm.flink.source.ClickSource;
import com.wmm.flink.transform.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;

import java.time.Duration;

/**
 * 移除器demo
 */
public class EvictorDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从自定义数据源读取数据，并提取时间戳、生成水位线
        SingleOutputStreamOperator<Event> stream = env
                .addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner((SerializableTimestampAssigner<Event>) (event, recordTimestamp) -> event.timestamp));

        stream.map((MapFunction<Event, Tuple2<String, Long>>) value -> Tuple2.of(value.user, 1L), Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy(r -> r.f0)
                //设置滚动事件时间窗口
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .evictor(new Evictor<Tuple2<String, Long>, TimeWindow>() {
                    @Override
                    public void evictBefore(Iterable<TimestampedValue<Tuple2<String, Long>>> elements, int size, TimeWindow window, EvictorContext evictorContext) {
                        if (elements.iterator().hasNext()) {
                            TimestampedValue<Tuple2<String, Long>> next = elements.iterator().next();
                            if (next.getValue().f0.equals("Mary")) {
                                elements.iterator().remove();
                            }
                        }
                    }

                    @Override
                    public void evictAfter(Iterable<TimestampedValue<Tuple2<String, Long>>> elements, int size, TimeWindow window, EvictorContext evictorContext) {

                    }
                })
                .reduce((ReduceFunction<Tuple2<String, Long>>) (value1, value2) -> Tuple2.of(value1.f0, value1.f1 + value2.f1))
                .print();

        env.execute();
    }
}
