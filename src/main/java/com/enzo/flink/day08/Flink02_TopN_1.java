package com.enzo.flink.day08;

import com.enzo.flink.bean.WaterSensor;
import com.enzo.flink.func.WaterSensorMapFunction;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * topN的第一种实现
 * 实时统计一段时间内的出现次数最多的水位。例如，统计最近10秒钟内出现次数最多的两个水位，并且每5秒钟更新一次
 * 窗口：滑动事件窗口
 * 大小：10s
 * 步长：5s
 */
public class Flink02_TopN_1 {
    public static void main(String[] args) throws Exception {
//        Configuration conf = new Configuration();
//        conf.setInteger(RestOptions.PORT, 12345);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.disableOperatorChaining();
        env.setParallelism(1);
        DataStreamSource<String> sockDS = env.socketTextStream("fastfood102", 8888);

        // 类型转换
        SingleOutputStreamOperator<WaterSensor> wsDS = sockDS.map(new WaterSensorMapFunction());

        SingleOutputStreamOperator<WaterSensor> withWatermarkDS = wsDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<WaterSensor>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<WaterSensor>() {
                                    @Override
                                    public long extractTimestamp(WaterSensor ws, long l) {
                                        return ws.getTs() * 1000;
                                    }
                                }
                        )
        );

        // 开窗
        AllWindowedStream<WaterSensor, TimeWindow> windowDS = withWatermarkDS.windowAll(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)));

        // 对窗口中的数据处理
        SingleOutputStreamOperator<String> processDS = windowDS.process(
                new ProcessAllWindowFunction<WaterSensor, String, TimeWindow>() {


                    @Override
                    public void process(ProcessAllWindowFunction<WaterSensor, String, TimeWindow>.Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {

                        // map要放在process里面，不然slot任务槽会重叠，要相互之间隔离开
                        Map<Integer, Integer> vcCountMap = new HashMap<>();

                        // 遍历所有窗口中的数据
                        for (WaterSensor ws : elements) {
                            // 获取当前数据的水位置
                            Integer vc = ws.getVc();
                            // 判断当前水位置是否采集过
                            if (vcCountMap.containsKey(vc)) {
                                vcCountMap.put(vc, vcCountMap.get(vc) + 1);
                            } else {
                                vcCountMap.put(vc, 1);
                            }
                        }
                        List<Tuple2<Integer, Integer>> vcCountList = new ArrayList<>();
                        for (Map.Entry<Integer, Integer> entry : vcCountMap.entrySet()) {
                            vcCountList.add(Tuple2.of(entry.getKey(), entry.getValue()));
                        }
                        vcCountList.sort((a, b) -> b.f1 - a.f1);

                        // 3.取出 count最大的2个 vc
                        StringBuilder outStr = new StringBuilder();

                        outStr.append("================================\n");
                        // 遍历 排序后的 List，取出前2个， 考虑可能List不够2个的情况  ==》 List中元素的个数 和 2 取最小值
                        for (int i = 0; i < Math.min(2, vcCountList.size()); i++) {
                            Tuple2<Integer, Integer> vcCount = vcCountList.get(i);
                            outStr.append("Top" + (i + 1) + "\n");
                            outStr.append("vc=" + vcCount.f0 + "\n");
                            outStr.append("count=" + vcCount.f1 + "\n");
                            outStr.append("窗口结束时间=" + DateFormatUtils.format(context.window().getEnd(), "yyyy-MM-dd HH:mm:ss.SSS") + "\n");
                            outStr.append("================================\n");
                        }

                        out.collect(outStr.toString());

                    }
                }
        );


        processDS.print();
        env.execute();


    }
}
