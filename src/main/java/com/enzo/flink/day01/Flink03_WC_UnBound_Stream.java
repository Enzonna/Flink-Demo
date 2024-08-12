package com.enzo.flink.day01;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 以流的形式对无界数据进行处理
 */
public class Flink03_WC_UnBound_Stream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env
                .socketTextStream("fastfood102", 8888)
                .flatMap(
                        (String lineStr, Collector<Tuple2<String, Long>> out) -> {
                            String[] words = lineStr.split(" ");
                            for (String word : words) {
                                out.collect(Tuple2.of(word, 1L));
                            }
                        }
                )
                // 如果使用lambda表达式创建实现类对象，返回值类型中存在泛型，会有泛型擦除的问题，可以通过returns方法指定返回值类型
                .returns(Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy(0)
                .sum(1)
                .print();

        env.execute();
    }
}
