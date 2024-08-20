package com.enzo.flink.day10;

import com.enzo.flink.bean.WaterSensor;
import com.enzo.flink.func.WaterSensorMapFunction;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Test {
    public static void main(String[] args) throws Exception {


        Configuration conf = new Configuration();
        conf.set(RestOptions.PORT, 12345);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        DataStreamSource<String> socketDS = env.socketTextStream("fastfood102", 8888);

        SingleOutputStreamOperator<WaterSensor> wsDS = socketDS.map(new WaterSensorMapFunction());

        SingleOutputStreamOperator<WaterSensor> withWatermarkDS = wsDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<WaterSensor>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<WaterSensor>() {
                                    @Override
                                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                                        return element.getTs() * 1000;
                                    }
                                }
                        )
        );

        KeyedStream<WaterSensor, Integer> vcKeyedDS = withWatermarkDS.keyBy(WaterSensor::getVc);


        WindowedStream<WaterSensor, Integer, TimeWindow> windowDS = vcKeyedDS.window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)));

        SingleOutputStreamOperator<Tuple3<Integer, Integer, Long>> aggregateDS = windowDS.aggregate(
                new AggregateFunction<WaterSensor, Integer, Integer>() {
                    @Override
                    public Integer createAccumulator() {
                        return 0;
                    }

                    @Override
                    public Integer add(WaterSensor waterSensor, Integer accumulator) {
                        return ++accumulator;
                    }

                    @Override
                    public Integer getResult(Integer accumulator) {
                        return accumulator;
                    }

                    @Override
                    public Integer merge(Integer integer, Integer acc1) {
                        return null;
                    }
                },
                new WindowFunction<Integer, Tuple3<Integer, Integer, Long>, Integer, TimeWindow>() {
                    @Override
                    public void apply(Integer vc, TimeWindow window, Iterable<Integer> input, Collector<Tuple3<Integer, Integer, Long>> out) throws Exception {
                        Integer count = input.iterator().next();
                        long end = window.getEnd();
                        out.collect(Tuple3.of(vc, count, end));
                    }
                }

        );


        KeyedStream<Tuple3<Integer, Integer, Long>, Long> endKeyedDS = aggregateDS.keyBy(t3 -> t3.f2);


        SingleOutputStreamOperator<String> processDS = endKeyedDS.process(
                new KeyedProcessFunction<Long, Tuple3<Integer, Integer, Long>, String>() {
                    final Map<Long, List<Tuple3<Integer, Integer, Long>>> vcCountMap = new HashMap<>();

                    @Override
                    public void processElement(Tuple3<Integer, Integer, Long> tup3, KeyedProcessFunction<Long, Tuple3<Integer, Integer, Long>, String>.Context ctx, Collector<String> out) throws Exception {
                        Long end = tup3.f2;
                        if (vcCountMap.containsKey(end)) {
                            vcCountMap.get(end).add(tup3);
                        } else {
                            List<Tuple3<Integer, Integer, Long>> vcCountList = new ArrayList<>();
                            vcCountList.add(tup3);
                            vcCountMap.put(end, vcCountList);
                        }
                        TimerService timerService = ctx.timerService();
                        timerService.registerEventTimeTimer(end + 1);
                    }

                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<Long, Tuple3<Integer, Integer, Long>, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                        Long end = ctx.getCurrentKey();
                        List<Tuple3<Integer, Integer, Long>> vcCountList = vcCountMap.get(end);
                        vcCountList.sort((t1, t2) -> t2.f1 - t1.f1);

                        StringBuilder outStr = new StringBuilder();

                        outStr.append("================================\n");
                        for (int i = 0; i < Math.min(2, vcCountList.size()); i++) {
                            Tuple3<Integer, Integer, Long> vcCount = vcCountList.get(i);
                            outStr.append("Top").append(i + 1).append("\n");
                            outStr.append("vc=").append(vcCount.f0).append("\n");
                            outStr.append("count=").append(vcCount.f1).append("\n");
                            outStr.append("窗口结束时间=").append(vcCount.f2).append("\n");
                            outStr.append("================================\n");
                        }

                        vcCountList.clear();

                        out.collect(outStr.toString());
                    }
                }
        );


        processDS.print();
        env.execute();


    }
}
