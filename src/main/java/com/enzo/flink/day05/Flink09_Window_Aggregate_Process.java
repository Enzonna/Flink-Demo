package com.enzo.flink.day05;

import com.enzo.flink.bean.WaterSensor;
import com.enzo.flink.func.WaterSensorMapFunction;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class Flink09_Window_Aggregate_Process {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> sockDS = env.socketTextStream("fastfood102", 8888);
        SingleOutputStreamOperator<WaterSensor> wsDS = sockDS.map(new WaterSensorMapFunction());
        KeyedStream<WaterSensor, String> keyedDS = wsDS.keyBy(data -> data.id);


        // 对分组后的数据进行开窗
        WindowedStream<WaterSensor, String, TimeWindow> windowDS
                = keyedDS.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));

        SingleOutputStreamOperator<String> aggregateDS = windowDS.aggregate(
                new AggregateFunction<WaterSensor, Integer, String>() {
                    @Override
                    public Integer createAccumulator() {
                        System.out.println("create✅✅✅");
                        return 0;
                    }

                    @Override
                    public Integer add(WaterSensor waterSensor, Integer integer) {
                        System.out.println("add✅✅✅");
                        return waterSensor.vc + integer;
                    }

                    @Override
                    public String getResult(Integer integer) {
                        System.out.println("getResult✅✅✅");
                        return integer + "";
                    }

                    @Override
                    public Integer merge(Integer integer, Integer acc1) {
                        System.out.println("merge✅✅✅");
                        return null;
                    }
                },
                new ProcessWindowFunction<String, String, String, TimeWindow>() {
                    @Override
                    public void process(String key, ProcessWindowFunction<String, String, String, TimeWindow>.Context context, Iterable<String> iterable, Collector<String> out) throws Exception {
                        String windowStart = DateFormatUtils.format(context.window().getStart(), "yyyy-MM-dd HH:mm:ss");
                        String windowEnd = DateFormatUtils.format(context.window().getEnd(), "yyyy-MM-dd HH:mm:ss");
                        String vc = iterable.iterator().next();
                        out.collect("当前传感器： " + key + " 窗口[" + windowStart + "," + windowEnd + ")" + "的vc总和为" + vc);


                    }
                }
        );


        aggregateDS.print();
        env.execute();
    }
}
