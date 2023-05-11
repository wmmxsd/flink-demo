package com.wmm.flink.windows;

import com.wmm.flink.common.UrlViewCountAgg;
import com.wmm.flink.common.UrlViewCountResult;
import com.wmm.flink.source.ClickSource;
import com.wmm.flink.transform.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * 增量聚合函数和全窗口函数联合使用demo
 * 统计 10 秒钟的 url 浏览量，每 5 秒钟更新一次，另外为了更加清晰地展示，还应该把窗口的起始结束时间一起输出
 */
public class UrlViewCountDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forMonotonousTimestamps()
                                .withTimestampAssigner((SerializableTimestampAssigner<Event>) (element, recordTimestamp) -> element.timestamp)
                );

        // 需要按照 url 分组，开滑动窗口统计
        stream.keyBy(data -> data.url)
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                //同时传入增量聚合函数和全窗口函数
                .aggregate(new UrlViewCountAgg(), new UrlViewCountResult())
                .print();

        env.execute();
    }

}
