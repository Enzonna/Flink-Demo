package com.enzo.flink.day03;


import com.enzo.flink.bean.WaterSensor;
import com.enzo.flink.func.MyMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Map;

/**
 * 转换算子-Map
 */
public class Flink09_Transform_Map {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<WaterSensor> stream = env.fromElements(
                new WaterSensor("sensor_1", 1L, 1),
                new WaterSensor("sensor_2", 2L, 2)
        );

        // 提取WaterSensor中的id字段
        // 方法一：stream.map(data -> data.id).print();
        // 方法二：stream.map(WaterSensor::getId).print();
        // 方法三：单独抽取类实现接口
        stream.map(
                new MyMapFunction()
        ).print();
        env.execute();
    }
}


