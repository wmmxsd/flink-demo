package com.wmm.flink.windows;

import com.wmm.flink.common.UrlViewCount;
import com.wmm.flink.common.UrlViewCountAgg;
import com.wmm.flink.common.UrlViewCountResult;
import com.wmm.flink.transform.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * 包含水位线的延迟，窗口允许迟到数据，以及将迟到数据放入窗口侧输出流的demo
 * 数据：
 * Alice, ./home, 1000
 * Alice, ./home, 2000
 * Alice, ./home, 10000
 * Alice, ./home, 9000
 * Alice, ./cart, 12000
 * Alice, ./prod?id=100, 15000
 * Alice, ./home, 9000
 * Alice, ./home, 8000
 * Alice, ./prod?id=200, 70000
 * Alice, ./home, 8000
 * Alice, ./prod?id=300, 72000
 * Alice, ./home, 8000
 */
public class UrlViewCount2Demo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env.socketTextStream("hadoop1", 8999)
                .map((MapFunction<String, Event>) value -> {
                    String[] fields = value.split(" ");
                    return new Event(fields[0].trim(), fields[1].trim(), Long.valueOf(fields[2].trim()));
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2L))
                                .withTimestampAssigner((element, recordTimestamp) -> element.timestamp)
                );

        // 定义侧输出流标签
        OutputTag<Event> outputTag = new OutputTag<Event>("late"){};

        SingleOutputStreamOperator<UrlViewCount> result = stream.keyBy(data -> data.url)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                // 方式二：允许窗口处理迟到数据，设置 1 分钟的等待时间
                .allowedLateness(Time.minutes(1))
                // 方式三：将最后的迟到数据输出到侧输出流
                .sideOutputLateData(outputTag)
                .aggregate(new UrlViewCountAgg(), new UrlViewCountResult());

        result.print("result");
        result.getSideOutput(outputTag).print("late");
        // 为方便观察，可以将原始数据也输出
        stream.print("input");
        env.execute();
    }
}
