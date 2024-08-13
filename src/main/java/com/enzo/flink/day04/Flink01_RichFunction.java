package com.enzo.flink.day04;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 富函数
 * 在流中数据进行处理的时候，相关算子要求传递一个处理函数实体作为参数
 * <p>
 * map(MapFunction),filter(FilterFunction),flatMap(FlatMapFunction)
 * <p>
 * 默认参数声明的形式是接口类型，其实除了接口之外，每一个声明的函数类，都有一个对应的富函数实现
 * <p>
 * 富函数的形式：Rich + 接口名 例如：MapFunction : RichMapFunction
 * <p>
 * 富函数和普通的接口比起来，多出了如下功能：
 * <p>
 * 提供了上下文对象，可以通过上下文对象，获取更多的信息
 * <p>
 * 提供了带生命周期的方法：
 * <p>
 * open：方法初始化的时候执行的代码
 * <p>
 * 在每一个算子子任务上”执行一次”
 * <p>
 * 一般用于进行连接的初始化
 *
 * <p>
 * close：方法结束的时候执行的代码
 * <p>
 * 在每个算子子任务上“执行一次”
 * <p>
 * 一般用于资源的释放操作，如果处理的是无界数据，close不会被执行
 */
public class Flink01_RichFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        DataStreamSource<String> ds = env.socketTextStream("fastfood102", 8888);

        // 富函数
        SingleOutputStreamOperator<String> mapDS = ds.map(
                new RichMapFunction<String, String>() {

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        System.out.println("^_^open被执行了");
                        // 运行的上下文
                        RuntimeContext runtimeContext = getRuntimeContext();
                        System.out.print("索引号 -> " + runtimeContext.getIndexOfThisSubtask() + "; ");
                        System.out.println("名称 -> " + runtimeContext.getTaskNameWithSubtasks());
                    }

                    @Override
                    public void close() throws Exception {
                        System.out.println("^_^close被执行了");
                    }

                    @Override
                    public String map(String s) throws Exception {
                        return "enzo" + s;
                    }
                }
        );

        mapDS.print();

        env.execute();


    }

}
