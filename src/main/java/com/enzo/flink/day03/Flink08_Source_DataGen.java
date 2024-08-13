package com.enzo.flink.day03;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * 自定义数据源
 */
public class Flink08_Source_DataGen {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 自定义数据源
        DataStreamSource<String> ds = env.addSource(new SourceFunction<String>() {

            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                for (int i = 0; i < 1000000; i++) {
                    ctx.collect("数据" + i);
                }
            }

            @Override
            public void cancel() {
                System.out.println("++++++++++++cancel被执行了+++++++++++");
            }
        });


        ds.print();
        env.execute();
    }
}
