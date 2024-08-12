package com.enzo.flink.day01;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 以流的形式对有界数据进行处理
 */
public class Flink02_WC_Bound_Stream {
    public static void main(String[] args) throws Exception {
        // 1. 指定流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 指定以批的方式进行流的处理 - 流批一体✅✅
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        // 2. 从指定文件中读取数据
        DataStreamSource<String> ds = env.readTextFile("F:\\atguigu\\22-Flink1.17\\flink-demo\\input\\word.txt");

        // 3. 扁平化处理，将原来的字符串转化为二元组
        SingleOutputStreamOperator<Tuple2<String, Long>> flatMap = ds.flatMap(
                new FlatMapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public void flatMap(String s, Collector<Tuple2<String, Long>> out) throws Exception {
                        String[] words = s.split(" ");
                        for (String word : words) {
                            out.collect(Tuple2.of(word, 1L));
                        }
                    }
                }
        );

        // 4. 按照单词进行分组
        KeyedStream<Tuple2<String, Long>, Tuple> keyedStream = flatMap.keyBy(0);

        // 5. 聚合计算
        SingleOutputStreamOperator<Tuple2<String, Long>> sum = keyedStream.sum(1);

        // 6. 将计算结果打印输出
        sum.print();

        // 7. 执行任务
        env.execute();
    }
}
