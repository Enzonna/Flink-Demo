package com.enzo.flink.myday03;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class My_File {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        FileSource<String> source = FileSource
                .forRecordStreamFormat(new TextLineInputFormat(), new Path("F:\\atg uigu\\22-Flink1.17\\flink-demo\\input\\word.txt"))
                .build();

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "enzo").print();
        env.execute();

    }


}
