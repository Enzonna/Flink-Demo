package com.enzo.flink.day03;


import com.enzo.flink.bean.WaterSensor;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 转换算子-flatMap
 * 如果输入的数据是sensor_1，只打印vc；如果输入的数据是sensor_2，既打印ts又打印vc
 */
public class Flink11_Transform_FlatMap {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<WaterSensor> stream = env.fromElements(
                new WaterSensor("sensor_1", 1L, 1),
                new WaterSensor("sensor_2", 2L, 2)
        );

        // map：通过方法的返回值将数据传递到下游，返回值只能有一个，所以只会向下游传递一条数据
        // flatMap：返回值是void，不是通过返回值将数据传递到下游，而是通过collect方法向下游传递数据，collect方法可以调用多次


//        stream.map(
//                new MapFunction<WaterSensor, String>() {
//                    @Override
//                    public String map(WaterSensor waterSensor) throws Exception {
//                        if ("sensor_1".equals(waterSensor.id)){
//                            return waterSensor.vc.toString();
//                        }else {
//                            return waterSensor.ts.toString();
//                            return waterSensor.vc.toString();
//                        }
//                    }
//                }
//        )

        stream.flatMap(
                new FlatMapFunction<WaterSensor, String>() {
                    @Override
                    public void flatMap(WaterSensor waterSensor, Collector<String> out) throws Exception {
                        if ("sensor_1".equals(waterSensor.id)) {
                            out.collect(waterSensor.vc.toString());
                        } else {
                            out.collect(waterSensor.ts.toString());
                            out.collect(waterSensor.vc.toString());
                        }
                    }
                }
        ).print();

        env.execute();
    }
}


