package com.enzo.flink.day05;


import com.enzo.flink.bean.WaterSensor;
import com.enzo.flink.func.WaterSensorMapFunction;
import org.apache.flink.api.common.functions.AggregateFunction;
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
 * 窗口处理函数-aggregate
 */
public class Flink07_Window_Aggregate {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> sockDS = env.socketTextStream("fastfood102", 8888);
        SingleOutputStreamOperator<WaterSensor> wsDS = sockDS.map(new WaterSensorMapFunction());
        KeyedStream<WaterSensor, String> keyedDS = wsDS.keyBy(data -> data.id);


        // 对分组后的数据进行开窗
        WindowedStream<WaterSensor, String, TimeWindow> windowDS
                = keyedDS.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));

        // 对当前窗口数据进行聚合计算
        SingleOutputStreamOperator<String> aggregateDS = windowDS.aggregate(
                new AggregateFunction<WaterSensor, Integer, String>() {
                    @Override
                    public Integer createAccumulator() {
                        System.out.println("createAccumulator✅");
                        return 0;
                    }

                    @Override
                    public Integer add(WaterSensor waterSensor, Integer integer) {
                        System.out.println("add✅");
                        return integer + waterSensor.getVc();
                    }

                    @Override
                    public String getResult(Integer integer) {
                        System.out.println("getResult✅");
                        return "水位和：" + integer;
                    }

                    @Override
                    public Integer merge(Integer integer, Integer acc1) {
                        System.out.println("merge✅");
                        return null;
                    }
                }
        );

        aggregateDS.print();
        env.execute();
    }
}
