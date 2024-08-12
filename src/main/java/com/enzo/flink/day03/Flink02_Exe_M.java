package com.enzo.flink.day03;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 执行模式
 * <p>
 * DataStreamAPI默认执行模式是STREAMING，流模式，流中每来一条数据就处理一次
 * <p>
 * BATCH：批模式，处理的是有界数据，将所有的数据收集完后处理一次
 * <p>
 * AUTOMATIC：根据数据来源自动设置流（无界）或者批（有界）
 * <p>
 * 两种方式设置运行模式：
 * <p>
 * 代码： env.setRuntimeMode(RuntimeExecutionMode.BATCH);
 * <p>
 * 在提交作业的时候，通过命令行参数设置：bin/flink run -Dexecution.runtime-mode=batch
 */
public class Flink02_Exe_M {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env
                .readTextFile("F:\\atguigu\\22-Flink1.17\\flink-demo\\input\\word.txt")
                .flatMap(
                        new FlatMapFunction<String, Tuple2<String, Long>>() {
                            @Override
                            public void flatMap(String s, Collector<Tuple2<String, Long>> out) throws Exception {
                                String[] words = s.split(" ");
                                for (String word : words) {
                                    out.collect(Tuple2.of(word, 1L));
                                }

                            }
                        }
                )
                .keyBy(0)
                .sum(1)
                .print();

        env.execute();
    }
}
