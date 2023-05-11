package com.wmm.flink.state.keyed;

import com.wmm.flink.source.ClickSource;
import com.wmm.flink.transform.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

/**
 * 算子状态-映射状态
 * 们模拟一个滚动窗口计算每一个 url 在每一个窗口中的 pv 数据
 */
public class MapStateDemo {
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

        // 统计每 10s 窗口内，每个 url 的 pv
        stream
                .keyBy(data -> data.url)
                .process(new KeyedProcessFunction<String, Event, String>() {
                    //窗口长度
                    private final Long windowSize = 10000L;
                    // 声明状态，用 map 保存 pv 值（窗口 start，count）
                    private MapState<Long, Long> windowPvMapState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        windowPvMapState = getRuntimeContext().getMapState(new MapStateDescriptor<>("window-pv", Long.class, Long.class));
                    }

                    @Override
                    public void processElement(Event value, KeyedProcessFunction<String, Event, String>.Context ctx, Collector<String> out) throws Exception {
                        // 每来一条数据，就根据时间戳判断属于哪个窗口
                        Long windowStart = value.timestamp / windowSize * windowSize;
                        long windowEnd = windowStart + windowSize;
                        // 注册 end -1 的定时器，窗口触发计算
                        ctx.timerService().registerEventTimeTimer(windowEnd - 1);
                        // 更新状态中的 pv 值
                        if (windowPvMapState.contains(windowStart)){
                            windowPvMapState.put(windowStart, windowPvMapState.get(windowStart) + 1);
                        } else {
                            windowPvMapState.put(windowStart, 1L);
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        long windowEnd = timestamp + 1;
                        Long windowStart = windowEnd - windowSize;
                        Long pv = windowPvMapState.get(windowStart);
                        out.collect(
                                "url: " + ctx.getCurrentKey()
                                + " 访问量: " + pv
                                + " 窗 口 ： " + new Timestamp(windowStart) + " ~ " + new Timestamp(windowEnd)
                        );
                        // 模拟窗口的销毁，清除 map 中的 key
                        windowPvMapState.remove(windowStart);
                    }
                })
                .print();
        env.execute();
    }
}
