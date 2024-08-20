package com.enzo.flink.day10;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 对检查点常用的配置
 */
public class Flink01_Checkpoint {
    public static void main(String[] args) throws Exception {
//        Configuration conf = new Configuration();
//        conf.set(RestOptions.PORT, 12345);
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 启用检查点
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();

        // 检查点存储
        // 默认状态后端hash，状态存在TM堆内存，检查点存在JM堆内存
        // env.setStateBackend(new HashMapStateBackend());
        // checkpointConfig.setCheckpointStorage(new JobManagerCheckpointStorage());
        checkpointConfig.setCheckpointStorage("hdfs://fastfood102:8020/ck");

        // 检查点模式（CheckpointingMode）
        //checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        // 超时时间（checkpointTimeout）
        checkpointConfig.setCheckpointTimeout(60000L);

        // 最小间隔时间（minPauseBetweenCheckpoints）
        checkpointConfig.setMinPauseBetweenCheckpoints(2000L);

        // 最大并发检查点数量（maxConcurrentCheckpoints）
        checkpointConfig.setMaxConcurrentCheckpoints(1);

        // 开启外部持久化存储（enableExternalizedCheckpoints）--job取消后，检查点是否保留
        checkpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        //检查点连续失败次数（tolerableCheckpointFailureNumber）
        checkpointConfig.setTolerableCheckpointFailureNumber(0);

        //在Flink中除了检查点之外，还提供了其它的容错手段，例如重启
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 3000L));
        //env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(30),Time.seconds(3000L)));

        // 非对齐检查点（enableUnalignedCheckpoints）
        // checkpointConfig.enableUnalignedCheckpoints();

        // 对齐检查点超时时间（alignedCheckpointTimeout）
        // checkpointConfig.setAlignedCheckpointTimeout(Duration.ofSeconds(10));

        //设置操作hadoop的用户
        System.setProperty("HADOOP_USER_NAME", "enzo");


        env
                .socketTextStream("fastfood102", 8888).uid("socket_uid")
                .flatMap(
                        (String lineStr, Collector<Tuple2<String, Long>> out) -> {
                            String[] words = lineStr.split(" ");
                            for (String word : words) {
                                out.collect(Tuple2.of(word, 1L));
                            }
                        }
                ).uid("flatMap_uid")
                // 如果使用lambda表达式创建实现类对象，返回值类型中存在泛型，会有泛型擦除的问题，可以通过returns方法指定返回值类型
                .returns(Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy(0)
                .sum(1).uid("sum_uid")
                .print().uid("print_uid");

        env.execute();
    }
}
