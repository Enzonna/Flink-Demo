package com.enzo.flink.day03;


import com.enzo.flink.bean.WaterSensor;
import com.enzo.flink.func.MyMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 转换算子-filter
 * 将数据流中传感器id为sensor_1的数据过滤出来
 */
public class Flink10_Transform_Filter {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<WaterSensor> stream = env.fromElements(
                new WaterSensor("sensor_1", 1L, 1),
                new WaterSensor("sensor_2", 2L, 2)
        );

        stream.filter(
                ws -> "sensor_1".equals(ws.id)
        ).print();
        env.execute();
    }
}


