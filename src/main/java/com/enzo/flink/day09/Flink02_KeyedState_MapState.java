package com.enzo.flink.day09;

import com.enzo.flink.bean.WaterSensor;
import com.enzo.flink.func.WaterSensorMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

public class Flink02_KeyedState_MapState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> socketDS = env.socketTextStream("fastfood102", 8888);
        SingleOutputStreamOperator<WaterSensor> wsDS = socketDS.map(new WaterSensorMapFunction());

        // 按照传感器的id分组
        KeyedStream<WaterSensor, String> keyedDS = wsDS.keyBy(WaterSensor::getId);

        // 统计每种传感器每种水位值出现的次数
        SingleOutputStreamOperator<String> processDS = keyedDS.process(
                new KeyedProcessFunction<String, WaterSensor, String>() {

                    MapState<Integer, Integer> vcCountMapState;

                    // 初始化赋值
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        vcCountMapState = getRuntimeContext().getMapState(new MapStateDescriptor<Integer, Integer>("vcCountMapState", Integer.class, Integer.class));

                    }

                    @Override
                    public void processElement(WaterSensor ws, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                        Integer vc = ws.getVc();
                        if (vcCountMapState.contains(vc)) {
                            vcCountMapState.put(vc, vcCountMapState.get(vc) + 1);
                        } else {
                            vcCountMapState.put(vc, 1);
                        }


                        // 2.遍历Map状态，输出每个k-v的值
                        StringBuilder outStr = new StringBuilder();
                        outStr.append("======================================\n");
                        outStr.append("传感器id为").append(ws.getId()).append("\n");
                        for (Map.Entry<Integer, Integer> vcCount : vcCountMapState.entries()) {
                            outStr.append(vcCount.toString()).append("\n");
                        }
                        outStr.append("======================================\n");

                        out.collect(outStr.toString());

                    }

                }
        );
        processDS.print();
        env.execute();

    }
}
