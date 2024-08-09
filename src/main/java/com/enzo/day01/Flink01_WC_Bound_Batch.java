package com.enzo.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * 以批的形式对有界数据进行处理
 */
public class Flink01_WC_Bound_Batch {
    public static void main(String[] args) throws Exception {
        // 1. 准备环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 2. 从文件中读取数据
        DataSource<String> ds = env.readTextFile("F:\\atguigu\\22-Flink1.17\\flink-demo\\input\\word.txt");

        // 3. 对读取的数据进行扁平化处理,封装成二元组
        FlatMapOperator<String, Tuple2<String, Long>> wordAndOne = ds.flatMap(
                new FlatMapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public void flatMap(String s, Collector<Tuple2<String, Long>> out) throws Exception {
                        // 使用分隔符对读取到的一行数据进行分割
                        String[] words = s.split(" ");

                        // 遍历数组获得每一个单词
                        for (String word : words) {
                            // 将单词封装为二元组并向下游传递
                            out.collect(Tuple2.of(word, 1L));
                        }
                    }
                }
        );

        // 4. 按照单词进行分组
        UnsortedGrouping<Tuple2<String, Long>> wordAndOneUG = wordAndOne.groupBy(0);

        // 5. 聚合计算
        AggregateOperator<Tuple2<String, Long>> sum = wordAndOneUG.sum(1);

        // 6. 将结果进行打印输出
        sum.print();
    }


}
