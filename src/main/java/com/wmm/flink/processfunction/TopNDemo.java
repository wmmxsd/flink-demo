package com.wmm.flink.processfunction;

import com.wmm.flink.common.UrlViewCount;
import com.wmm.flink.common.UrlViewCountAgg;
import com.wmm.flink.common.UrlViewCountResult;
import com.wmm.flink.source.ClickSource;
import com.wmm.flink.transform.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * 实时统计一段时间内的热门 url。例如，需要统计最近 10 秒钟内最热门的两个 url 链接，并且每 5 秒钟更新一次。
 */
public class TopNDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //读取源数据并且按照url分组，分组后求出每个url的访问次数
        SingleOutputStreamOperator<UrlViewCount> urlViewCountSingleOutputStreamOperator = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forMonotonousTimestamps()
                                .withTimestampAssigner((element, recordTimestamp) -> element.timestamp)
                )
                .keyBy(value -> value.url)
                .window(SlidingEventTimeWindows.of(Time.seconds(10L), Time.seconds(5L)))
                .aggregate(new UrlViewCountAgg(), new UrlViewCountResult());

        //按照窗口结束时间分组后每一组包含特定时间段的所有被访问的url信息，然后对每一组按照访问次数倒序排序。
        urlViewCountSingleOutputStreamOperator.keyBy(value -> value.windowEnd)
                .process(new TopN(2))
                .print("result");

        env.execute();
    }

}
