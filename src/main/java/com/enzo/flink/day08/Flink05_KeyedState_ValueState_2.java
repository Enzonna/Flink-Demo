package com.enzo.flink.day08;


import com.enzo.flink.bean.WaterSensor;
import com.enzo.flink.func.WaterSensorMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
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
public class Flink05_KeyedState_ValueState_2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> socketDS = env.socketTextStream("fastfood102", 8888);
        SingleOutputStreamOperator<WaterSensor> wsDS = socketDS.map(new WaterSensorMapFunction());

        // 按照传感器的id分组
        KeyedStream<WaterSensor, String> keyedDS = wsDS.keyBy(WaterSensor::getId);


        keyedDS.process(
                new KeyedProcessFunction<String, WaterSensor, String>() {
                    // 声明键控状态，注意和普通变量作用范围不同，键控状态的作用范围就是keyBy后的每个组
                    ValueState<Integer> lastVcState;


                    // 注意：不能在声明的时候直接赋值，因为这个时候还获取不到运行时上下文
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        // 状态赋值
                        ValueStateDescriptor<Integer> valueStateDescriptor
                                = new ValueStateDescriptor<Integer>("lastVcState", Integer.class);
                        lastVcState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public void processElement(WaterSensor ws, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                        // 获取当前水位值
                        Integer curVc = ws.getVc();
                        // 获取上次水位值
                        Integer lastVc = lastVcState.value() == null ? 0 : lastVcState.value();

                        if (Math.abs(curVc - lastVc) > 10) {
                            out.collect("传感器" + ctx.getCurrentKey() + "当前水位" + curVc + "与上一个水位" + lastVc + "差值超过10");
                        }

                        // 更新当前水位值到状态中
                        lastVcState.update(curVc);
                    }
                }
        ).printToErr();


        env.execute();

    }
}
