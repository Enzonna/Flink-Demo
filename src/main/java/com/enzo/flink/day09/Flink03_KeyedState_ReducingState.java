package com.enzo.flink.day09;

import com.enzo.flink.bean.WaterSensor;
import com.enzo.flink.func.WaterSensorMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class Flink03_KeyedState_ReducingState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> socketDS = env.socketTextStream("fastfood102", 8888);
        SingleOutputStreamOperator<WaterSensor> wsDS = socketDS.map(new WaterSensorMapFunction());

        // 按照传感器的id分组
        KeyedStream<WaterSensor, String> keyedDS = wsDS.keyBy(WaterSensor::getId);

        // 计算每种传感器的水位和
        keyedDS.process(
                new KeyedProcessFunction<String, WaterSensor, String>() {

                    // 规约
                    ReducingState<Integer> vcSumStates;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        vcSumStates = getRuntimeContext().getReducingState(new ReducingStateDescriptor<Integer>("vcSumStates",
                                new ReduceFunction<Integer>() {
                                    @Override
                                    public Integer reduce(Integer integer, Integer t1) throws Exception {
                                        return integer + t1;
                                    }
                                },
                                Integer.class));
                    }

                    @Override
                    public void processElement(WaterSensor ws, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                        Integer vc = ws.getVc();
                        vcSumStates.add(vc);
                        out.collect(ctx.getCurrentKey() + "水位和为：" + vcSumStates.get());
                    }
                }
        ).print();

        env.execute();


    }
}
