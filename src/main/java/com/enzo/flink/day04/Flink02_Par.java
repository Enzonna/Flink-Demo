package com.enzo.flink.day04;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * 分区操作（算子）
 */
public class Flink02_Par {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set(RestOptions.PORT, 8088);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(4);
        // env.disableOperatorChaining();   禁用算子链
        DataStreamSource<String> ds = env.socketTextStream("fastfood102", 8888);

        // 1. shuffle 随机分区
        // ds.shuffle().print;

        // 2. rebalance 轮询分区
        // ds.rebalance().print;

        // 3. rescale 缩放分区,当调用rescale()方法时，其实底层也是使用Round-Robin算法进行轮询，但是只会将数据轮询发送到下游并行任务的一部分中
        // rescale的做法是分成小团体，发牌人只给自己团体内的所有人轮流发牌
        // ds.rescale().print;

        // 4. broadcast 广播分区
        // ds.broadcast().print();

        // 5. global 全局分区
        // ds.global().print();

        // 6. keyBy
        ds.keyBy(data -> data).print();

        // 7. one-2-one forward
        // ds.print();

        // 8. 自定义   custom
//        ds.partitionCustom(
//                new MyPar(),
//                data -> data
//        ).print();


        env.execute();

    }
}


class MyPar implements Partitioner<String> {
    @Override
    public int partition(String key, int i) {
        return key.hashCode() % i;
    }
}