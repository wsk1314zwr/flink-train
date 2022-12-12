package com.wsk.flink13.java.state;

import com.wsk.flink13.java.function.CountWindowAverage;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @description: stage 有ValueState、ReducingState、ListState、AggregatingState、MapState
 * 自定义基于计数窗口的 求窗口平均数的分析
 * @author: wsk
 * @date: 2021/6/22 17:41
 * @version: 1.0
 */
public class CountWindowAverageTest {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.fromElements(Tuple2.of(1L, 3L), Tuple2.of(1L, 5L), Tuple2.of(1L, 7L), Tuple2.of(1L, 4L), Tuple2.of(1L, 2L), Tuple2.of(1L, 0L))
                .keyBy(value -> value.f0)
                .flatMap(new CountWindowAverage())
                .print();

        env.execute();
    }
}
