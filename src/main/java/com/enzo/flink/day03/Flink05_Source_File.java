package com.enzo.flink.day03;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 从文件中读取数据
 */
public class Flink05_Source_File {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 从文件中读取数据
        FileSource<String> source = FileSource
                .forRecordStreamFormat(new TextLineInputFormat(), new Path("F:\\atguigu\\22-Flink1.17\\flink-demo\\input\\word.txt"))
                .build();
        // 封装为流（从指定的数据流读取数据，将其封装为流）
        // flink1.12前
        // env.addSource()
        // flink1.12后
        DataStreamSource<String> ds = env.fromSource(source, WatermarkStrategy.noWatermarks(), "my_fileSource");
        ds.print();
        env.execute();

    }
}
