package com.wmm.flink.partition;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/**
 * 分区-重缩放分区
 */
public class RescaleDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //这里使用了并行数据源的富函数版本，这样可以调用 getRuntimeContext 方法来获取运行时上下文的一些信息
        env.addSource(new RichParallelSourceFunction<Integer>() {
                    @Override
                    public void run(SourceContext<Integer> sourceContext) {
                        for (int i = 0; i < 8; i++) {
                            // 将奇数发送到索引为 1 的并行子任务， 将偶数发送到索引为 0 的并行子任务
                            if ((i + 1) % 2 == getRuntimeContext().getIndexOfThisSubtask()) {
                                sourceContext.collect(i + 1);
                            }
                        }
                    }

                    @Override
                    public void cancel() {
                    }
                })
                .setParallelism(2)
                .rescale()
                .print().setParallelism(4);
        env.execute();
    }
}
