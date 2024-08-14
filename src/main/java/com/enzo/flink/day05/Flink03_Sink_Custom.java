package com.enzo.flink.day05;


import com.enzo.flink.bean.WaterSensor;
import com.enzo.flink.func.MySinkFunction;
import com.enzo.flink.func.WaterSensorMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 * 自定义Sink
 */
public class Flink03_Sink_Custom {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> sockDS = env.socketTextStream("fastfood102", 8888);
        SingleOutputStreamOperator<WaterSensor> wsDS = sockDS.map(new WaterSensorMapFunction());


        wsDS.addSink(
                new MySinkFunction()
        );
        env.execute();
    }
}
