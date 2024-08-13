package com.enzo.flink.day04;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 合流算子-Union
 */
public class Flink05_Union {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set(RestOptions.PORT, 8088);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        DataStreamSource<String> ds1 = env.socketTextStream("fastfood102", 8888);
        DataStreamSource<String> ds2 = env.socketTextStream("fastfood102", 9999);
        DataStreamSource<String> ds3 = env.fromElements("hello", "world");

        // 合流的数据类型必须一致
        ds1.union(ds2).print();

        env.execute();
    }
}
