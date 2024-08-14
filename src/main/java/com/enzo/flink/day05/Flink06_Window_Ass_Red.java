package com.enzo.flink.day05;


import com.enzo.flink.bean.WaterSensor;
import com.enzo.flink.func.WaterSensorMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * 窗口的创建以及对窗口中数据的处理
 * <p>
 * 需求：每小时统计一次不同传感器采集的水位和
 */
public class Flink06_Window_Ass_Red {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> sockDS = env.socketTextStream("fastfood102", 8888);
        SingleOutputStreamOperator<WaterSensor> wsDS = sockDS.map(new WaterSensorMapFunction());
        KeyedStream<WaterSensor, String> keyedDS = wsDS.keyBy(data -> data.id);

        /*
        以滚动处理时间窗口为例：
            什么时候创建窗口对象？
                当有属于这个窗口的第一个元素到来的时候，创建窗口对象
            窗口对象的起始时间和结束时间？
                窗口对象是左闭右开的[ )
                起始时间：向下取整
                结束时间：向下取整 + 窗口长度
                最大时间：结束时间 - 1ms
            窗口如何触发计算？


         */


        // 对分组后的数据进行开窗
        WindowedStream<WaterSensor, String, TimeWindow> windowDS
                = keyedDS.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));

        // 对当前窗口数据进行聚合计算
        SingleOutputStreamOperator<WaterSensor> reduceDS = windowDS.reduce(
                new ReduceFunction<WaterSensor>() {
                    @Override
                    public WaterSensor reduce(WaterSensor waterSensor, WaterSensor t1) throws Exception {
                        System.out.println("中间累加结果 = " + waterSensor);
                        System.out.println("新来的数据 = " + t1);
                        waterSensor.setVc(waterSensor.getVc() + t1.getVc());
                        return waterSensor;
                    }
                }
        );

        reduceDS.print();
        env.execute();
    }
}
