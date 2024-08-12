package com.enzo.flink.day03;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * 从集合中来读取数据
 */
public class Flink04_Source_Col {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // DataStreamSource<Integer> ds = env.fromCollection(Arrays.asList(1, 2, 3, 4, 5));
        DataStreamSource<Integer> ds = env.fromElements(1, 2, 3, 4, 5);
        ds.print();
        env.execute();
    }
}
