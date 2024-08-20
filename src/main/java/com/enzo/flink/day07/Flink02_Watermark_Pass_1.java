package com.enzo.flink.day07;

import com.enzo.flink.bean.WaterSensor;
import com.enzo.flink.func.WaterSensorMapFunction;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * 水位线传递
 */
public class Flink02_Watermark_Pass_1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        DataStreamSource<String> sockDS = env.socketTextStream("fastfood102", 8888);

        // 设置水位线的生成策略以及提取事件时间字段
        SingleOutputStreamOperator<String> withWatermarkDS = sockDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<String>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<String>() {
                                    @Override
                                    public long extractTimestamp(String s, long l) {
                                        return Long.valueOf(s.split(",")[1]);
                                    }
                                }
                        )
                        // 空闲数据源，这里时间就是处理时间
                        .withIdleness(Duration.ofSeconds(10))
        );

        // 对流中的数据进行类型转换
        SingleOutputStreamOperator<WaterSensor> mapDS = withWatermarkDS.map(
                new WaterSensorMapFunction()
        );

        // 按照传感器id进行分组
        KeyedStream<WaterSensor, String> keyedDS = mapDS.keyBy(WaterSensor::getId);

        // 开窗
        WindowedStream<WaterSensor, String, TimeWindow> windowDS = keyedDS.window(TumblingEventTimeWindows.of(Time.milliseconds(10)));

        // 聚合处理
        SingleOutputStreamOperator<String> processDS = windowDS.process(
                new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<WaterSensor, String, String, TimeWindow>.Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                        String windowStart = DateFormatUtils.format(context.window().getStart(), "yyyy-MM-dd HH:mm:ss.SSS");
                        String windowEnd = DateFormatUtils.format(context.window().getEnd(), "yyyy-MM-dd HH:mm:ss.SSS");
                        long count = elements.spliterator().estimateSize();
                        out.collect("key=" + s + "的窗口[" + windowStart + "," + windowEnd + ")包含" + count + "条数据===>" + elements.toString());
                    }
                }
        );

        processDS.print();
        env.execute();
    }
}
