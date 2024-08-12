package com.enzo.flink.day03;


import com.enzo.flink.bean.WaterSensor;
import com.enzo.flink.func.WaterSensorMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 聚合算子-reduce
 */
public class Flink14_Transform_Reduce {
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

        // 如果流中只有一条数据，reduce方法不会执行
        // reduce(value1,value2)    value1:累加的结果，value2:流中新来的数据
        SingleOutputStreamOperator<WaterSensor> reduceDS = keyedDS.reduce(
                new ReduceFunction<WaterSensor>() {
                    @Override
                    public WaterSensor reduce(WaterSensor v1, WaterSensor v2) throws Exception {
                        System.out.println("v1 = " + v1);
                        System.out.println("v2 = " + v2);
                        v1.setVc(v1.vc + v2.vc);
                        return v1;
                    }
                }
        );

        reduceDS.print("++++");
        env.execute();

    }
}


