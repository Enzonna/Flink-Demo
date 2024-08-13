package com.enzo.flink.day03;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Flink执行环境的创建
 */
public class Flink01_Env {
    public static void main(String[] args) throws Exception {
        // 1.创建本地执行环境
        // StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

        // 2.创建本地执行环境 带WebUI，需要添加flink-runtime-web依赖
        // StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        // 3.创建远程执行环境
        // StreamExecutionEnvironment env
        //        = StreamExecutionEnvironment.createRemoteEnvironment("fastfood102", 8081, "F:\\atguigu\\22-Flink1.17\\flink-demo\\target\\flink-demo-1.0-SNAPSHOT.jar");

        // 4.根据实际执行的场景自动创建远程或者本地环境✅✅✅
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 注意：如果通过 StreamExecutionEnvironment.getExecutionEnvironment(conf)获取本地带WebUI的执行环境
        // 必须在conf中显式的执行WebUI的端口号
//        Configuration conf = new Configuration();
//        conf.setInteger(RestOptions.PORT, 8088);
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        DataStreamSource<String> ds = env.readTextFile("/opt/module/flink-1.17.0/word.txt");
        ds.print();
        env.execute();

    }
}
