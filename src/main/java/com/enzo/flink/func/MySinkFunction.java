package com.enzo.flink.func;


import com.enzo.flink.bean.WaterSensor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * 将流中数据写入MySQL中
 */
public class MySinkFunction extends RichSinkFunction<WaterSensor> {

    Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {
        // 注册驱动
        Class.forName("com.mysql.cj.jdbc.Driver");

        // 建立连接
        connection = DriverManager.getConnection("jdbc:mysql://fastfood102:3306/flink_test?serverTimezone=Asia/Shanghai&useUnicode=true&characterEncoding=UTF-8",
                "root",
                "000000");
    }

    @Override
    public void close() throws Exception {
        if (connection != null) {
            connection.close();
        }
    }

    @Override
    public void invoke(WaterSensor value, Context context) throws Exception {

        // 获取数据库操作对象
        String sql = "insert into ws values(?,?,?)";
        PreparedStatement ps = connection.prepareStatement(sql);
        ps.setString(1, value.getId());
        ps.setLong(2, value.getTs());
        ps.setString(3, value.getVc() + "");

        // 执行SQL
        ps.execute();

        // 释放资源
        if (ps != null) {
            ps.close();
        }

    }

}
