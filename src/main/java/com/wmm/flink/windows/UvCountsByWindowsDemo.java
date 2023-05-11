package com.wmm.flink.windows;

import com.wmm.flink.source.ClickSource;
import com.wmm.flink.transform.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.HashSet;

/**
 * 全窗口函数-处理窗口函数
 * 电商网站统计每小时 UV（独立访客数）
 */
public class UvCountsByWindowsDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(1L))
                                .withTimestampAssigner((element, recordTimestamp) -> element.timestamp)
                )
                .keyBy(value -> true)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10L)))
                .process(new ProcessWindowFunction<Event, String, Boolean, TimeWindow>() {
                    @Override
                    public void process(Boolean aBoolean, ProcessWindowFunction<Event, String, Boolean, TimeWindow>.Context context, Iterable<Event> elements, Collector<String> out) throws Exception {
                        HashSet<String> userSet = new HashSet<>();
                        // 遍历所有数据，放到 Set 里去重
                        for (Event event: elements){
                            userSet.add(event.user);
                        }
                        // 结合窗口信息，包装输出内容
                        TimeWindow window = context.window();
                        out.collect(" 窗 口 : " + new Timestamp(window.getStart()) + " ~ " + new Timestamp(window.getEnd()) + " 的独立访客数量是：" + userSet.size());
                    }
                })
                .print();

        env.execute();
    }
}
