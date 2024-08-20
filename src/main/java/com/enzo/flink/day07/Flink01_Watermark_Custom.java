package com.enzo.flink.day07;

import com.enzo.flink.bean.WaterSensor;
import com.enzo.flink.func.WaterSensorMapFunction;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.*;
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
 * Watermark生成策略,自定义水位线
 */
public class Flink01_Watermark_Custom {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> sockDS = env.socketTextStream("fastfood102", 8888);
        SingleOutputStreamOperator<WaterSensor> wsDS = sockDS.map(new WaterSensorMapFunction());

        // 指定watermark的生成策略以及提取时间字段
        SingleOutputStreamOperator<WaterSensor> withWatermarkDS = wsDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.forGenerator(
                        new WatermarkGeneratorSupplier<WaterSensor>() {
                            @Override
                            public WatermarkGenerator<WaterSensor> createWatermarkGenerator(Context context) {
                                return new MyGen(5);
                            }
                        }
                ).withTimestampAssigner(
                        new SerializableTimestampAssigner<WaterSensor>() {
                            @Override
                            public long extractTimestamp(WaterSensor waterSensor, long l) {
                                return waterSensor.getTs();
                            }
                        }
                )
        );

        KeyedStream<WaterSensor, String> keyedDS = withWatermarkDS.keyBy(WaterSensor::getId);

        WindowedStream<WaterSensor, String, TimeWindow> windowDS = keyedDS.window(TumblingEventTimeWindows.of(Time.milliseconds(10)));

        SingleOutputStreamOperator<String> processDS = windowDS.process(
                new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                        String windowStart = DateFormatUtils.format(context.window().getStart(), "yyyy-MM-dd HH:mm:ss");
                        String windowEnd = DateFormatUtils.format(context.window().getEnd(), "yyyy-MM-dd HH:mm:ss");
                        long count = elements.spliterator().estimateSize();
                        out.collect("key=" + s + "的窗口[" + windowStart + "," + windowEnd + ")包含" + count + "条数据===>" + elements.toString());
                    }
                }
        );


        processDS.print();
        env.execute();
    }
}

class MyGen implements WatermarkGenerator<WaterSensor> {
    long maxTs;
    long outOfTx;

    public MyGen(long outOfTx) {
        this.outOfTx = outOfTx;
    }

    // 流中每来一条数据都会调用一次该方法
    @Override
    public void onEvent(WaterSensor event, long enventTimestamp, WatermarkOutput output) {
        maxTs = Math.max(maxTs, enventTimestamp);
    }

    // 周期性执行的方法
    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        // 看是否严格递增
        output.emitWatermark(new Watermark(maxTs - outOfTx - 1));

    }
}
