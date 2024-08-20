package com.enzo.flink.day08;


import com.enzo.flink.bean.WaterSensor;
import com.enzo.flink.func.WaterSensorMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

/**
 * 值状态
 * 需求：检测每种传感器的水位值，如果连续的两个水位差值超过10，就输出报警
 */
public class Flink04_KeyedState_ValueState_1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> socketDS = env.socketTextStream("fastfood102", 8888);
        SingleOutputStreamOperator<WaterSensor> wsDS = socketDS.map(new WaterSensorMapFunction());

        // 按照传感器的id分组
        KeyedStream<WaterSensor, String> keyedDS = wsDS.keyBy(WaterSensor::getId);


        // 判断连续的两个水位差是否超过10
        /*
        keyedDS.process(
                new KeyedProcessFunction<String, WaterSensor, String>() {
                    // 成员变量的作用范围，每一个并行子任务（slot），有可能被同一个slot下的多个组共享
                    Integer lastVc = 0;

                    @Override
                    public void processElement(WaterSensor ws, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                        Integer curVc = ws.getVc();
                        if (Math.abs(curVc - lastVc) > 10) {
                            out.collect("传感器" + ctx.getCurrentKey() + "当前水位" + curVc + "与上一个水位" + lastVc + "差值超过10");
                        }
                        lastVc = curVc;
                    }
                }
        ).printToErr();
        */

        keyedDS.process(
                new KeyedProcessFunction<String, WaterSensor, String>() {
                    // 成员变量的作用范围，每一个并行子任务（slot），有可能被同一个slot下的多个组共享
                    Map<String, Integer> lastVcMap = new HashMap<>();

                    @Override
                    public void processElement(WaterSensor ws, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                        String id = ws.getId();
                        Integer lastVc = lastVcMap.get(id) == null ? 0 : lastVcMap.get(id);
                        // 获取当前水位值
                        Integer curVc = ws.getVc();
                        if (Math.abs(curVc - lastVc) > 10) {
                            out.collect("传感器" + ctx.getCurrentKey() + "当前水位" + curVc + "与上一个水位" + lastVc + "差值超过10");
                        }
                        lastVcMap.put(id, curVc);
                    }
                }
        ).printToErr();


        env.execute();

    }
}
