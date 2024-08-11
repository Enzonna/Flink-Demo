package com.enzo.day02;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 算子链
 * 在Flink中，并行度相同的一对一（one to one）算子操作，可以直接链接在一起形成一个“大”的任务（task）
 * 这样原来的算子就成为了真正任务里的一部分，这样的技术被称为“算子链”（Operator Chain）
 * 算子链是Flink提供一种非常有效的优化手段，不需要我们做什么处理，默认就会进行合并
 * 前提：
 * 算子间必须是one-to-one的关系
 * 并行度相同
 * 不能存在重分区操作
 * <p>
 * 禁用算子链
 * <p>
 * 全局禁用算子链 env.disableOperatorChaining();
 * <p>
 * 算子禁用  算子.disableChaining()
 * <p>
 * 开始新链  算子..startNewChain()
 */
public class Flink02_OpeChain {
    public static void main(String[] args) throws Exception {
        // 1. 指定流处理环境
        // StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Configuration conf = new Configuration();
        conf.set(RestOptions.PORT, 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        env.setParallelism(2);

        // 2. 从指定的网络端口读取数据
        DataStreamSource<String> sockDS = env.socketTextStream("fastfood102", 8888);

        // 3. 对读取的数据进行扁平化处理
        SingleOutputStreamOperator<String> wordDS = sockDS.flatMap(
                new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String s, Collector<String> out) throws Exception {
                        String[] wordArr = s.split(" ");
                        for (String word : wordArr) {
                            out.collect(word);
                        }
                    }
                }
        );
        // 4. 将流中数据转换为二元组
        SingleOutputStreamOperator<Tuple2<String, Long>> tupDS = wordDS.map(
                new MapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String s) throws Exception {
                        return Tuple2.of(s, 1L);
                    }
                }
        );

        // 5. 按照单词进行分组
        KeyedStream<Tuple2<String, Long>, Tuple> keyedDS = tupDS.keyBy(0);

        // 6. 聚合计算
        SingleOutputStreamOperator<Tuple2<String, Long>> sumDS = keyedDS.sum(1);

        // 7. 打印输出
        sumDS.print();

        // 8. 提交作业
        env.execute();
    }
}
