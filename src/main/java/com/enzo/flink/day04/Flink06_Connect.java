package com.enzo.flink.day04;


import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * 合流算子-connect
 */
public class Flink06_Connect {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set(RestOptions.PORT, 8089);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        DataStreamSource<String> ds1 = env.socketTextStream("fastfood102", 8888);
        SingleOutputStreamOperator<Integer> ds2 = env.socketTextStream("fastfood102", 9999).map(Integer::valueOf);

        // 只能对两条流进行合并
        ConnectedStreams<String, Integer> connectDS = ds1.connect(ds2);

        SingleOutputStreamOperator<String> mapDS = connectDS.map(new CoMapFunction<String, Integer, String>() {
            @Override
            public String map1(String value) throws Exception {
                // 处理第一条流的数据
                return "字符串 -> " + value;
            }

            @Override
            public String map2(Integer value) throws Exception {
                // 处理第二条流的数据
                return "整数 -> " + value;
            }
        });


        mapDS.print();

        env.execute();
    }
}
