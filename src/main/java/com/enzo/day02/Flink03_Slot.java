package com.enzo.day02;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 该案例演示了任务槽Slot
 * TaskManager是一个JVM进程，开始开启多个线程执行任务，每一个线程叫做任务槽
 * <p>
 * 任务槽可以均分TaskManager的内存资源
 * <p>
 * 在flink-conf.yaml的配置文件中可以设置一个tm上slot的数量
 * <p>
 * taskmanager.numberOfTaskSlots: 1
 * <p>
 * 在设置slot数量的时候，除了要考虑内存资源外，也需要考虑CPU情况，一般slot的数量和cpu核数之间的关系是1:1或者2:1
 * <p>
 * slot共享:
 * <p>
 * 只要属于同一个作业，那么对于不同任务节点（算子）的并行子任务，就可以放到同一个slot上执行
 */
public class Flink03_Slot {
    public static void main(String[] args) throws Exception {
        //TODO 1.指定流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //Configuration conf = new Configuration();
        //conf.set(RestOptions.PORT,8081);
        //StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        //env.setParallelism(2);

        //全局禁用算子链
        //env.disableOperatorChaining();

        //TODO 2.从指定的网络端口读取数据
        DataStreamSource<String> sockDS = env.socketTextStream("fastfood102", 8888);
        //TODO 3.对读取的数据进行扁平化处理
        SingleOutputStreamOperator<String> wordDS = sockDS.flatMap(
                new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String lineStr, Collector<String> out) throws Exception {
                        String[] wordArr = lineStr.split(" ");
                        for (String word : wordArr) {
                            out.collect(word);
                        }
                    }
                }
        ).slotSharingGroup("bbb");
        //).disableChaining();
        //).startNewChain();
        //TODO 4.将流中数据转换为二元组
        SingleOutputStreamOperator<Tuple2<String, Long>> tupDS = wordDS.map(
                new MapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String word) throws Exception {
                        return Tuple2.of(word, 1L);
                    }
                }
        ).slotSharingGroup("aaa");
        //TODO 5.按照单词进行分组
        KeyedStream<Tuple2<String, Long>, Tuple> keyedDS = tupDS.keyBy(0);
        //TODO 6.聚合计算
        SingleOutputStreamOperator<Tuple2<String, Long>> sumDS = keyedDS.sum(1);
        //TODO 7.打印输出
        sumDS.print();
        //TODO 8.提交作业
        env.execute();

    }
}
