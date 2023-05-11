package com.wmm.flink;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;


/**
 *  无界数据流
 */
public class UnBoundedStreamWordCount {
    public static void main(String[] args) throws Exception {
        // 1. 创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2. 读取文件
        DataStreamSource<String> txtStreamSource = env.socketTextStream("hadoop1", 8999);
        // 3. 转换数据格式
        SingleOutputStreamOperator<Tuple2<String, Long>> wordAndOne = txtStreamSource
                .flatMap((String line, Collector<String> words) -> Arrays.stream(line.split(" ")).forEach(words::collect), BasicTypeInfo.STRING_TYPE_INFO)
                .setParallelism(4)
                .map(value -> Tuple2.of(value, 1L), Types.TUPLE(Types.STRING, Types.LONG))
                .setParallelism(2);
        // 4. 分组
        KeyedStream<Tuple2<String, Long>, String> wordAndOneKS = wordAndOne.keyBy(t -> t.f0);
        // 5. 求和
        SingleOutputStreamOperator<Tuple2<String, Long>> result = wordAndOneKS.sum(1);
        // 6. 打印
        result.print();
        // 7. 执行
        env.execute();
    }
}
