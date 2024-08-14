package com.enzo.flink.day05;

import com.enzo.flink.bean.WaterSensor;
import com.enzo.flink.func.WaterSensorMapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 窗口API的概述
 */
public class Flink04_Window_API {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SingleOutputStreamOperator<WaterSensor> wsDS = env
                .socketTextStream("fastfood102", 8888)
                .map(new WaterSensorMapFunction());

        // 窗口API
        // 开窗前是否进行keyBy操作
        // 没有keyBy，针对整条流进行开窗，相当于并行度设置为1
        // wsDS.windowAll()
        // wsDS.countWindowAll()

        // 有了keyBy，针对keyBy之后的每一组进行单独开窗，窗口之间互相不影响
        // wsDS.keyBy().windowAll();
        // wsDS.keyBy().countWindowAll();


        /*窗口分配器 -> 开什么样的窗口
            window(WindowAssigner)
            countWindow(窗口大小)  ->  滚动窗口
            countWindow(窗口大小，滑动步长) ->  滑动窗口
         */


        /*窗口处理函数 -> 如何处理窗口中的函数
            1.增量处理函数 -> 每来一条数据，窗口中数据都会被调用一次，不会对数据进行缓存，选哪个就看类型✅✅
            优点：不会缓存数据，省空间
            缺点：获取不到窗口更详细的信息
                reduce() 窗口中的元素的类型以及向下游传递的类型是一致的，如果窗口中只有一个元素，reduce方法不会被执行
                    reduce(value1,value2)
                        value1:中间累加结果
                        value2:新来的数据

                aggregate() 窗口中元素的类型、累加器的类型以及向下游传递的数据的类型可以不一致
                    createAccumulator:属于当前窗口的第一个元素进来的时候执行
                    add():窗口中每来一条数据都会被调用一次，会缓存数据
                    getResult():窗口触发计算后，会调用一次，会将缓存的数据全部处理完

            2.全量处理函数 -> 每来一条数据，窗口中数据都会被调用一次，会缓存数据，
            在触发窗口计算前，会将数据缓存起来，在触发窗口计算后，会将缓存的数据全部处理完
            优点：获取窗口更详细的信息
            缺点：会缓存数据，比较占空间
              apply()
              apply(String key, TimeWindow window, Iterable<WaterSensor> input, Collector<String> out)

              process()
              process(String key, Context context, Iterable<WaterSensor> iterable, Collector<String> out)
              process更底层，通过content对象除了可以获取窗口对象外，还可以获取更丰富的信息


             在实际使用中，可以增量➕全量
                reduce + apply
                reduce + process
                aggregate + apply
                aggregate + process
         */


        // 窗口触发器 -> 何时触发窗口的计算
        //      .trigger()

        // 窗口移除器 -> 在窗口触发计算后，在处理函数执行前或者后执行一些移除操作
        //      .evictor()


    }
}
