package com.enzo.flink.day05;

import com.enzo.flink.bean.WaterSensor;
import com.enzo.flink.func.WaterSensorMapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.*;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * 窗口分配器
 */
public class Flink05_Window_Assi {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> wsDS = env
                .socketTextStream("fastfood102", 8888)
                .map(new WaterSensorMapFunction());

        wsDS.keyBy(data -> data.id);
        // 滚动处理时间窗口
        // .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))

        // 滑动处理时间窗口
        // .window(TumblingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)))

        // 处理时间会话窗口
        // .window(ProcessingTimeSessionWindows.withGap(Time.seconds(10)))

        // 滚动事件时间窗口
        // .window(TumblingEventTimeWindows.of(Time.seconds(10)))

        // 滑动事件时间窗口
        // .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))

        // 事件时间会话窗口
        // .window(EventTimeSessionWindows.withGap(Time.seconds(10)))

        // 滚动计数窗口
        // .countWindow(5)

        // 滑动计数窗口
        // .countWindow(5, 2)

        // 全局窗口
        // .window(GlobalWindows.create())


    }
}
