package com.wmm.flink.sink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/**
 * 数据输出到文件
 */
public class FileSinkDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop1", 8999);

        StreamingFileSink<String> fileSink = getStringStreamingFileSink();

        socketTextStream
                .flatMap((FlatMapFunction<String, String>) (value, out) -> Arrays.stream(value.split(" ")).forEach(out::collect), Types.STRING)
                //.flatMap((String line, Collector<String> words) -> Arrays.stream(line.split(" ")).forEach(words::collect), BasicTypeInfo.STRING_TYPE_INFO)
                .map((MapFunction<String, Tuple2<String, Long>>) value -> Tuple2.of(value, 1L), Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy(value -> value.f0)
                .sum(1)
                .map(Tuple2::toString)
                .addSink(fileSink);

        env.execute();
    }

    private static StreamingFileSink<String> getStringStreamingFileSink() {
        return StreamingFileSink
                .<String>forRowFormat(new Path("./output"), new SimpleStringEncoder<>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy
                                .builder()
                                //至少包含15分钟的数据
                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
                                //最近5分钟没有收到数据
                                .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
                                //文件大小已经达到1GB
                                .withMaxPartSize(1024 * 1024 * 1024)
                                .build()
                )
                .build();
    }
}
