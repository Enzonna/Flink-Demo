package com.enzo.flink.day10;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.ProducerConfig;

/**
 * 两阶段提交——Sink
 * 如果将Flink流中的数据写到Kafka主题中，要想保证写入的精准一次，需要做如下操作
 * 开启检查点
 * 设置一致性级别为精准一次
 * .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
 * 设置事务id的前缀
 * .setTransactionalIdPrefix("2pc_sink")
 * 设置事务的超时时间
 * 检查点超时时间 < 事务的超时时间 <= 事务的最大超时时间
 * 在消费端设置消费隔离级别
 * .setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG,"read_committed")
 */
public class Flink02_2pc_Sink {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 开启检查点
        env.enableCheckpointing(10000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000L);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 3000L));

        DataStreamSource<String> socketDS = env.socketTextStream("fastfood102", 8888);


        // 将流中的数据写到kafka主题
        KafkaSink<String> kafkaDS = KafkaSink.<String>builder()
                .setBootstrapServers("fastfood102:9092")
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic("first")
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix("2pc_sink")
                .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 15 * 60 * 1000 + "")
                .build();

        socketDS.sinkTo(kafkaDS);

        env.execute();

    }
}
