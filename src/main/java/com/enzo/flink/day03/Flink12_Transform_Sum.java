package com.enzo.flink.day03;


import com.enzo.flink.bean.WaterSensor;
import com.enzo.flink.func.WaterSensorMapFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 聚合算子-sum
 */
public class Flink12_Transform_Sum {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> ds = env.socketTextStream("fastfood102", 8888);

        // 将流中数据进行类型转换 string -> WaterSensor
        SingleOutputStreamOperator<WaterSensor> wsDS = ds.map(
                new WaterSensorMapFunction()
        );

        // 按照传感器id进行分组
        KeyedStream<WaterSensor, String> keyedDS = wsDS.keyBy(WaterSensor::getId);

        // 求和
        SingleOutputStreamOperator<WaterSensor> sumDS = keyedDS.sum("vc");

        sumDS.print();
        env.execute();

    }
}


