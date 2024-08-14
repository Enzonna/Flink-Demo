package com.enzo.flink.day05;

import com.enzo.flink.bean.WaterSensor;
import com.enzo.flink.func.WaterSensorMapFunction;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

/**
 * 将流的数据写入kafka不同的主题中
 */
public class Flink01_Kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> sockDS = env.socketTextStream("fastfood102", 8888);
        SingleOutputStreamOperator<WaterSensor> wsDS = sockDS.map(
                new WaterSensorMapFunction()
        );

        KafkaSink<WaterSensor> kafkaSink = KafkaSink
                .<WaterSensor>builder()
                .setBootstrapServers("fastfood102:9092")
                .setRecordSerializer(
                        new KafkaRecordSerializationSchema<WaterSensor>() {
                            @Nullable
                            @Override
                            public ProducerRecord<byte[], byte[]> serialize(WaterSensor ws, KafkaSinkContext kafkaSinkContext, Long aLong) {
                                return new ProducerRecord<byte[], byte[]>(ws.id, ws.toString().getBytes());
                            }
                        }
                )
                .build();

        wsDS.sinkTo(kafkaSink);
        env.execute();
    }
}
