package com.enzo.flink.day06;

import com.enzo.flink.bean.WaterSensor;
import com.enzo.flink.func.WaterSensorMapFunction;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SessionWindowTimeGapExtractor;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 处理时间的会话窗口
 */
public class Flink03_Window_ProcessTime_Session {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> sockDS = env.socketTextStream("fastfood102", 8888);
        SingleOutputStreamOperator<WaterSensor> wsDS = sockDS.map(new WaterSensorMapFunction());
        KeyedStream<WaterSensor, String> keyedDS = wsDS.keyBy(WaterSensor::getId);

        // withGap 静态时间限制
        //WindowedStream<WaterSensor, String, TimeWindow> windowDS = keyedDS.window(ProcessingTimeSessionWindows.withGap(Time.seconds(10)));
        // 动态
        WindowedStream<WaterSensor, String, TimeWindow> windowDS = keyedDS.window(
                ProcessingTimeSessionWindows.withDynamicGap(
                        new SessionWindowTimeGapExtractor<WaterSensor>() {
                            @Override
                            public long extract(WaterSensor waterSensor) {
                                return waterSensor.getTs() * 1000;
                            }
                        }
                )
        );
        SingleOutputStreamOperator<String> processDS = windowDS.process(
                new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<WaterSensor, String, String, TimeWindow>.Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
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
