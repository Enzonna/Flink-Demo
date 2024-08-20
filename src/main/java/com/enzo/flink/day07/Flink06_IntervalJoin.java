package com.enzo.flink.day07;

import com.enzo.flink.bean.Dept;
import com.enzo.flink.bean.Emp;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * 该案例演示了基于状态实现的双流join
 */
public class Flink06_IntervalJoin {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        //从指定的网络端口中读取员工数据,并指定Watermark的生成策略以及提取事件时间字段
        SingleOutputStreamOperator<Emp> empDS = env
                .socketTextStream("fastfood102", 8888)
                .map(
                        new MapFunction<String, Emp>() {
                            @Override
                            public Emp map(String lineStr) throws Exception {
                                String[] filedArr = lineStr.split(",");
                                return new Emp(Integer.valueOf(filedArr[0]), filedArr[1], Integer.valueOf(filedArr[2]), Long.valueOf(filedArr[3]));
                            }
                        }
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Emp>forMonotonousTimestamps()
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<Emp>() {
                                            @Override
                                            public long extractTimestamp(Emp emp, long recordTimestamp) {
                                                return emp.getTs();
                                            }
                                        }
                                )
                );


        //从指定的网络端口中读取部门数据,并指定Watermark的生成策略以及提取事件时间字段
        SingleOutputStreamOperator<Dept> deptDS = env
                .socketTextStream("fastfood102", 8889)
                .map(
                        new MapFunction<String, Dept>() {
                            @Override
                            public Dept map(String lineStr) throws Exception {
                                String[] filedArr = lineStr.split(",");
                                return new Dept(Integer.valueOf(filedArr[0]), filedArr[1], Long.valueOf(filedArr[2]));
                            }
                        }
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Dept>forMonotonousTimestamps()
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<Dept>() {
                                            @Override
                                            public long extractTimestamp(Dept dept, long recordTimestamp) {
                                                return dept.getTs();
                                            }
                                        }
                                )
                );

        //使用IntervalJoin关联2条流
        SingleOutputStreamOperator<Tuple2<Emp, Dept>> joinedDS = empDS
                .keyBy(Emp::getDeptno)
                .intervalJoin(deptDS.keyBy(Dept::getDeptno))
                .between(Time.milliseconds(-5), Time.milliseconds(5))
                .process(
                        new ProcessJoinFunction<Emp, Dept, Tuple2<Emp, Dept>>() {
                            @Override
                            public void processElement(Emp left, Dept right, ProcessJoinFunction<Emp, Dept, Tuple2<Emp, Dept>>.Context ctx, Collector<Tuple2<Emp, Dept>> out) throws Exception {
                                out.collect(Tuple2.of(left, right));
                            }
                        }
                );


        joinedDS.print();
        env.execute();
    }
}
