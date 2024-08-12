package com.enzo.flink.day03;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Flink作业的提交
 */
public class Flink03_submit {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> sockDS = env.socketTextStream("fastfood102", 8888);
        sockDS.print();
        // 以同步的方式提交作业
        // env.execute();
        // 以异步的方式提交作业
        env.executeAsync();

        //-----------------------------------------------------
        StreamExecutionEnvironment env1 = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> sockDS1 = env1.socketTextStream("fastfood102", 8889);
        sockDS1.print();
        // env1.execute();
        env1.executeAsync();
    }
}
