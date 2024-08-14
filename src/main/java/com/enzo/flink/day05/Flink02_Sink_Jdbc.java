package com.enzo.flink.day05;

import com.enzo.flink.bean.WaterSensor;
import com.enzo.flink.func.WaterSensorMapFunction;
import org.apache.flink.connector.jdbc.*;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class Flink02_Sink_Jdbc {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> sockDS = env.socketTextStream("fastfood102", 8888);
        SingleOutputStreamOperator<WaterSensor> wsDS = sockDS.map(new WaterSensorMapFunction());

        SinkFunction<WaterSensor> jdbcSink = JdbcSink.<WaterSensor>sink(
                "insert into ws values(?,?,?)",
                new JdbcStatementBuilder<WaterSensor>() {
                    @Override
                    public void accept(PreparedStatement ps, WaterSensor ws) throws SQLException {
                        // 给问号占位符赋值
                        ps.setString(1, ws.getId());
                        ps.setLong(2, ws.getTs());
                        ps.setString(3, ws.getVc() + "");
                    }
                },
                // 攒批是针对单个并行子任务而言的
                JdbcExecutionOptions.builder()
                        // 攒批的大小
                        .withBatchSize(5)
                        // 攒批的时间
                        .withBatchIntervalMs(3000)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName("com.mysql.cj.jdbc.Driver")
                        .withUrl("jdbc:mysql://fastfood102:3306/flink_test?serverTimezone=Asia/Shanghai&useUnicode=true&characterEncoding=UTF-8")
                        .withUsername("root")
                        .withPassword("000000")
                        .build()
        );

        wsDS.addSink(jdbcSink);
        env.execute();
    }
}
