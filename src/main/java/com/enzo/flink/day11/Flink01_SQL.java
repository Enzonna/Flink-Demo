package com.enzo.flink.day11;


import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * 该案例演示了FlinkSQL相关API操作
 * 基本环境准备
 * 流--->动态表--->持续查询--->动态表--->写入到外部系统
 * --->流
 * 提交作业
 */
public class Flink01_SQL {
    public static void main(String[] args) throws Exception {
        // TODO 基本环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 指定表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // TODO 流--->动态表
        //2.1 通过执行建表语句
        tableEnv.executeSql("CREATE TABLE source ( \n" +
                "    id INT, \n" +
                "    ts BIGINT, \n" +
                "    vc INT\n" +
                ") WITH ( \n" +
                "    'connector' = 'datagen', \n" +
                "    'rows-per-second'='1', \n" +
                "    'fields.id.kind'='random', \n" +
                "    'fields.id.min'='1', \n" +
                "    'fields.id.max'='10', \n" +
                "    'fields.ts.kind'='sequence', \n" +
                "    'fields.ts.start'='1', \n" +
                "    'fields.ts.end'='1000000', \n" +
                "    'fields.vc.kind'='random', \n" +
                "    'fields.vc.min'='1', \n" +
                "    'fields.vc.max'='100'\n" +
                ");\n");
        // DataStreamSource<Integer> ds = env.fromElements(1, 2, 3, 4);

        // 2.2 通过API的方式
        // Table numTable = tableEnv.fromDataStream(ds,$("num"));

        // TODO 查询
        // 3.1 执行SQL的方式进行查询
        // executeSql方法执行查询语句的话，返回是TableResult,可以直接对表结果进行处理
        // tableEnv.executeSql("select * from source").print();

        // sqlQuery方法执行查询语句的话，返回是Table表对象,不能直接得到结果，仅仅是一个表对象，可以对当前对象做进一步的处理
        // 也可以调用表对象的.execute()方法获取结果集
        // tableEnv.sqlQuery("select * from source").execute().print();

        // 这里直接执行会报错，因为numTable还没有注册到表执行环境中❌❌❌
        // tableEnv.executeSql("select * from numTable").print();

        // 注册表对象到表执行环境中
        // tableEnv.createTemporaryView("num_table", numTable);
        // tableEnv.executeSql("select * from num_table").print();
        // tableEnv.executeSql("select * from " + numTable).print();

        // 3.2 通过Table API方式进行查询
        // Table sourceTable = tableEnv.sqlQuery("select * from source where id=1");
        // Table sourceTable = tableEnv.sqlQuery("select * from source");
        // sourceTable
        //         .select($("id"),$("ts"),$("vc"))
        //         .where($("id").isEqual(1))
        //         .execute()
        //         .print();

        // TODO 将动态表写入到外部系统
        // 4.1 执行SQL语句
        tableEnv.executeSql("CREATE TABLE sink (\n" +
                "    id INT, \n" +
                "    ts BIGINT, \n" +
                "    vc INT\n" +
                ") WITH (\n" +
                "'connector' = 'print'\n" +
                ")");
        // tableEnv.executeSql("insert into sink select * from source");
        // 4.2 TableAPI
        Table sourceTable = tableEnv.sqlQuery("select * from source");
        // sourceTable.executeInsert("sink");

        // TODO 将动态表转换为流
        DataStream<Row> ds = tableEnv.toDataStream(sourceTable);
        ds.print();
        // DataStream<WaterSensor> ds = tableEnv.toDataStream(sourceTable, WaterSensor.class);

        // TODO 提交作业
        // 如果在程序中最后的是流，通过env.execute()方法提交作业
        // 注意：如果在程序中最后的是动态表，不需要通过env.execute()方法提交作业
        env.execute();
    }
}
