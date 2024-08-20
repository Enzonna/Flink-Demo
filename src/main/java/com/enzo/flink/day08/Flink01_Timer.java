package com.enzo.flink.day08;

import com.enzo.flink.bean.WaterSensor;
import com.enzo.flink.func.WaterSensorMapFunction;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class Flink01_Timer {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        DataStreamSource<String> sockDS = env.socketTextStream("fastfood102", 8888);

        SingleOutputStreamOperator<WaterSensor> wsDS = sockDS.map(
                new WaterSensorMapFunction()
        );

        // 指定watermark生成策略，并提取事件时间字段
        SingleOutputStreamOperator<WaterSensor> withWatermarkDS = wsDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<WaterSensor>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<WaterSensor>() {
                                    @Override
                                    public long extractTimestamp(WaterSensor waterSensor, long l) {
                                        return waterSensor.getTs();
                                    }
                                }
                        )
        );


        KeyedStream<WaterSensor, String> keyedDS = withWatermarkDS.keyBy(data -> data.id);

        // 演示定时器使用
        SingleOutputStreamOperator<String> processDS = keyedDS.process(
                new KeyedProcessFunction<String, WaterSensor, String>() {
                    @Override
                    public void processElement(WaterSensor ws, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                        // 获取当前分组的key
                        String currentKey = ctx.getCurrentKey();
                        System.out.println("currentKey = " + currentKey);

                        // 获取当前数据的事件时间
                        Long timestamp = ctx.timestamp();
                        System.out.println("timestamp = " + timestamp);

                        // 注意：使用测输出流，必须使用process算子，因为只有在process的底层才能调用ctx的output
                        // ctx.output

                        // 获取定时服务
                        TimerService timerService = ctx.timerService();
                        // System.out.println("timerService = " + timerService);
                        // 获取当前水位线
                        long currentWatermark = timerService.currentWatermark();
                        System.out.println("当前水位线： = " + currentWatermark);
                        // 获取当前处理时间
                        long currentProcessingTime = timerService.currentProcessingTime();
                        System.out.println("当前处理时间 = " + currentProcessingTime);

                        // 注册处理时间定时器
                        // timerService.registerProcessingTimeTimer();
                        // 注册事件时间定时器
                        timerService.registerEventTimeTimer(10);

                        // 删除处理时间定时器
                        // timerService.deleteProcessingTimeTimer();
                        // 删除事件时间定时器
                        // timerService.deleteEventTimeTimer(10);

                        System.out.println("------------------------------------------");
                    }

                    // 当定时器被触发的时候执行该方法
                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<String, WaterSensor, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                        System.out.println("传感器key是" + ctx.getCurrentKey() + "的定时器被触发了" + timestamp);
                        System.out.println("==========================================");
                    }
                }
        );

        processDS.print();
        env.execute();


    }
}
