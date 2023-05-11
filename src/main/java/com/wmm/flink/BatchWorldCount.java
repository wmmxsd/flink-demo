package com.wmm.flink;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * 批处理
 */
public class BatchWorldCount {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> stringDataSource = env.readTextFile(String.valueOf(BatchWorldCount.class.getResource("/word.txt")));

        FlatMapOperator<String, Tuple2<String, Long>> wordAndOne = stringDataSource.flatMap((String value, Collector<Tuple2<String, Long>> out) -> {
            String[] wordArr = value.split(" ");
            for (String word : wordArr) {
                out.collect(Tuple2.of(word, 1L));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.LONG));

        UnsortedGrouping<Tuple2<String, Long>> wordAndOneUG = wordAndOne.groupBy(0);

        AggregateOperator<Tuple2<String, Long>> sum = wordAndOneUG.sum(1);

        sum.print();
    }
}
