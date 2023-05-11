package com.wmm.flink.multiflowconversion.mergeflow;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * 合流-连接操作
 */
public class ConnectDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        
        DataStream<Integer> stream1 = env.fromElements(1,2,3);
        DataStream<Long> stream2 = env.fromElements(1L,2L,3L);
        
        ConnectedStreams<Integer, Long> connectedStreams = stream1.connect(stream2);
        connectedStreams.map(new CoMapFunction<Integer, Long, String>() {
            @Override
            public String map1(Integer value) throws Exception {
                return "Integer: " + value;
            }

            @Override
            public String map2(Long value) throws Exception {
                return "Long: " + value;
            }
        }).print();

        env.execute();
    }
}
