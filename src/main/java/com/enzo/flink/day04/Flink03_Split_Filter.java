package com.enzo.flink.day04;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 分流-filter
 */
public class Flink03_Split_Filter {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set(RestOptions.PORT, 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        SingleOutputStreamOperator<Integer> socketDS = env
                .socketTextStream("fastfood102", 8888)
                .map(Integer::valueOf);

        // 过滤出奇数流
        SingleOutputStreamOperator<Integer> ds1 = socketDS.filter(data -> data % 2 == 1);

        // 过滤出偶数流
        SingleOutputStreamOperator<Integer> ds2 = socketDS.filter(data -> data % 2 == 0);

        ds1.print("奇数 -> ");
        ds2.print("偶数 -> ");

        env.execute();
    }
}
