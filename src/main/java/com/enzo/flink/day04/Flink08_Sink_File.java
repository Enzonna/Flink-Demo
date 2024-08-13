package com.enzo.flink.day04;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.time.Duration;

/**
 * Sink输出到文件
 */
public class Flink08_Sink_File {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 开启检查点
        env.enableCheckpointing(5000L);

        // 数据生成器
        DataStreamSource<String> ds = env.fromSource(
                new DataGeneratorSource<String>(
                        new GeneratorFunction<Long, String>() {
                            @Override
                            public String map(Long value) throws Exception {
                                return "数据 -> " + value;
                            }
                        },
                        Long.MAX_VALUE,
                        RateLimiterStrategy.perSecond(100),
                        Types.STRING
                ),
                WatermarkStrategy.noWatermarks(),
                "enzo"
        );

        // 将流中数据写到文件中
        FileSink<String> ds1 = FileSink
                .forRowFormat(new Path("F://output//"), new SimpleStringEncoder<String>("UTF-8"))
                // 配置文件名，文件的前缀和后缀
                .withOutputFileConfig(new OutputFileConfig("enzo-", ".log"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                // 15分钟滚动一次
                                .withRolloverInterval(Duration.ofMinutes(15))
                                // 5分钟不活跃（5min没有写数据）滚动一次
                                .withInactivityInterval(Duration.ofMinutes(5))
                                // 达到指定的大小滚动一次
                                .withMaxPartSize(new MemorySize(1024))
                                .build())
                .build();


        ds.sinkTo(ds1);
        env.execute();

    }
}
