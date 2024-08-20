package com.enzo.flink.day09;

import com.enzo.flink.bean.WaterSensor;
import com.enzo.flink.func.WaterSensorMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class Flink01_KeyedState_ListState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> socketDS = env.socketTextStream("fastfood102", 8888);
        SingleOutputStreamOperator<WaterSensor> wsDS = socketDS.map(new WaterSensorMapFunction());

        // 按照传感器的id分组
        KeyedStream<WaterSensor, String> keyedDS = wsDS.keyBy(WaterSensor::getId);

        // 每种传感器最高的三个水位值
        SingleOutputStreamOperator<String> processDS = keyedDS.process(
                new KeyedProcessFunction<String, WaterSensor, String>() {
                    // 定义listState，存放最高的三个水位值
                    ListState<Integer> vcListState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        vcListState = getRuntimeContext().getListState(new ListStateDescriptor<Integer>("vcListState", Integer.class));
                    }

                    @Override
                    public void processElement(WaterSensor ws, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                        // 获取当前采集的水位值
                        Integer curVc = ws.getVc();
                        // 将采集到的水位值放入状态中
                        vcListState.add(curVc);
                        // 排序
                        List<Integer> vcList = new ArrayList();
                        for (Integer vc : vcListState.get()) {
                            vcList.add(vc);
                        }
                        vcList.sort((o1, o2) -> o2 - o1);
                        if (vcList.size() > 3) {
                            vcList.remove(3);
                        }
                        vcListState.update(vcList);

                        out.collect("传感器" + ws.getId() + "的最高3个水位值：" + vcList.toString());
                    }
                }
        );
        processDS.print();
        env.execute();

    }
}
