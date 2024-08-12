package com.enzo.flink.day03;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 数据生成器连接器，主要生成一些测试数据
 */
public class Flink07_Source_DataGen {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 通过数据生成器生成数据
        DataStreamSource<String> ds = env.fromSource(
                new DataGeneratorSource<String>(
                        new GeneratorFunction<Long, String>() {
                            @Override
                            public String map(Long value) throws Exception {
                                return "数据 -> " + value;
                            }
                        },
                        100,
                        RateLimiterStrategy.perSecond(2),   // 每秒
                        Types.STRING
                ),
                WatermarkStrategy.noWatermarks(),
                "data_gen");

        ds.print();
        env.execute();
    }
}
