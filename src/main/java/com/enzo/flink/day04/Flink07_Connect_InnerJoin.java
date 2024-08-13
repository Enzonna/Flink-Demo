package com.enzo.flink.day04;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 内连接
 */
public class Flink07_Connect_InnerJoin {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set(RestOptions.PORT, 8085);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        DataStreamSource<Tuple2<Integer, String>> ds1 = env.fromElements(
                Tuple2.of(1, "a1"),
                Tuple2.of(1, "a2"),
                Tuple2.of(2, "b"),
                Tuple2.of(3, "c")
        );
        DataStreamSource<Tuple3<Integer, String, Integer>> ds2 = env.fromElements(
                Tuple3.of(1, "aa1", 1),
                Tuple3.of(1, "aa2", 2),
                Tuple3.of(2, "bb", 1),
                Tuple3.of(3, "cc", 1)
        );

        // 使用connect将两条流合并在一起
        ConnectedStreams<Tuple2<Integer, String>, Tuple3<Integer, String, Integer>> connectDS = ds1.connect(ds2);

        // 按照id进行分组
        // ConnectedStreams<Tuple2<Integer, String>, Tuple3<Integer, String, Integer>> keyedDS = connectDS.keyBy(0, 0);
        ConnectedStreams<Tuple2<Integer, String>, Tuple3<Integer, String, Integer>> keyedDS = connectDS.keyBy(
                tup2 -> tup2.f0,
                tup3 -> tup3.f0
        );

        // 对分组关联后的数据进行处理
        SingleOutputStreamOperator<String> process = keyedDS.process(
                new KeyedCoProcessFunction<Integer, Tuple2<Integer, String>, Tuple3<Integer, String, Integer>, String>() {
                    private Map<Integer, List<Tuple2<Integer, String>>> ds1Cache = new HashMap<>();
                    private Map<Integer, List<Tuple3<Integer, String, Integer>>> ds2Cache = new HashMap<>();

                    @Override
                    public void processElement1(Tuple2<Integer, String> tup2, KeyedCoProcessFunction<Integer, Tuple2<Integer, String>, Tuple3<Integer, String, Integer>, String>.Context ctx, Collector<String> out) throws Exception {
                        // 通过流中二元组对象获取id
                        Integer id = tup2.f0;
                        // 先将当前数据放入缓存中
                        if (ds1Cache.containsKey(id)) {
                            // 说明缓存中已经存在当前id对应的数据
                            ds1Cache.get(id).add(tup2);
                        } else {
                            // 说明缓存中没当前id对应的数据
                            List<Tuple2<Integer, String>> ds1List = new ArrayList<>();
                            ds1List.add(tup2);
                            ds1Cache.put(id, ds1List);
                        }

                        // 用当前数据和另外一条流中已经缓存的数据进行关联
                        if (ds2Cache.containsKey(id)) {
                            // 将另外一条流缓存中当前id对应的list取出来，并且进行遍历
                            for (Tuple3<Integer, String, Integer> tup3 : ds2Cache.get(id)) {
                                out.collect(tup2 + " -> " + tup3);
                            }
                        }
                    }

                    @Override
                    public void processElement2(Tuple3<Integer, String, Integer> tup3, KeyedCoProcessFunction<Integer, Tuple2<Integer, String>, Tuple3<Integer, String, Integer>, String>.Context ctx, Collector<String> out) throws Exception {
                        // 通过流中的三元组对象获取id
                        Integer id = tup3.f0;
                        // 先将当前数据放入缓存中
                        if (ds2Cache.containsKey(id)) {
                            // 说明缓存中已经存在当前id对应的数据
                            ds2Cache.get(id).add(tup3);
                        } else {
                            // 说明缓存中没当前id对应的数据
                            List<Tuple3<Integer, String, Integer>> ds2List = new ArrayList<>();
                            ds2List.add(tup3);
                            ds2Cache.put(id, ds2List);
                        }

                        // 用当前数据和另外一条流中已经缓存的数据进行关联
                        if (ds1Cache.containsKey(id)) {
                            // 将另外一条流缓存中当前id对应的list取出来，并且进行遍历
                            for (Tuple2<Integer, String> tup2 : ds1Cache.get(id)) {
                                out.collect(tup2 + " -> " + tup3);
                            }
                        }
                    }
                }
        );

        process.print();

        env.execute();


    }
}
