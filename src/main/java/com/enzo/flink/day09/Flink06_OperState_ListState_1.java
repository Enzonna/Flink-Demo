package com.enzo.flink.day09;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class Flink06_OperState_ListState_1 {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set(RestOptions.PORT, 12345);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);
        DataStreamSource<String> socketDS = env.socketTextStream("fastfood102", 8888);

        // 在map算子中的每个并行度上计算数据的个数
        socketDS.process(
                new ProcessFunction<String, String>() {

                    // 存储每个并行子任务计算元素的个数
                    Integer count = 0;

                    @Override
                    public void processElement(String value, ProcessFunction<String, String>.Context ctx, Collector<String> out) throws Exception {
                        out.collect(getRuntimeContext().getIndexOfThisSubtask() + "计算了" + ++count + "个元素");
                    }
                }
        ).print();
        env.execute();

    }
}
