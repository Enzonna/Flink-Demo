package com.enzo.flink.day03;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

/**
 * 从kafka主题中读取数据
 */
public class Flink06_Source_Kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从kafka主题中读取数据
        // 1. 创建kafkaSource对象
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("fastfood102:9092")
                .setTopics("first")
                .setGroupId("test")

                // 从最早位点开始消费
                // .setStartingOffsets(OffsetsInitializer.earliest())
                // 从最末尾位点开始消费
                .setStartingOffsets(OffsetsInitializer.latest())
                // 从时间戳大于等于指定时间戳（毫秒）的数据开始消费
                // .setStartingOffsets(OffsetsInitializer.timestamp(1657256176000L))
                // 从消费组提交的位点开始消费，不指定位点重置策略
                // .setStartingOffsets(OffsetsInitializer.committedOffsets())
                // 从消费组提交的位点开始消费，如果提交位点不存在，使用最早位点
                // .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))

                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // 2.封装流
        env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka source")
                .print();

        env.execute();

    }
}
