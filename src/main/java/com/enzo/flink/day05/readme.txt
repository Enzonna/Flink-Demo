
Jdbc连接器
    SinkFunction jdbcSinkFunction = JdbcSink.sink(
        sql,
        给问号占位符赋值,
        攒批配置(可选),
        连接选项
    );
    流.addSink(jdbcSinkFunction)
构造者设计模式
    对象创建和赋值一步搞定
    链式调用
自定义Sink
    class 类 implements SinkFunction|extends RichSinkFunction{
        invoke:向不同外部系统写入数据的逻辑
    }

时间语义
    事件时间:数据真正产生的时间
    处理时间:Flink中算子对数据进行处理的操作
    摄入时间(了解):数据进入到Flink的Source时间

    从Flink1.12开始，默认时间语义事件时间

窗口
    将无限的流数据划分一个个有限的数据库进行处理，就是所谓的窗口
    在理解窗口的时候，窗口是"桶"不是"框"

窗口分类
    按照驱动方式分
        时间窗口:以时间作为窗口起始或者结束的标记
        计数窗口:以元素的个数作为窗口的结束标记
    按照数据划分的方式分
        滚动窗口
            特点:窗口和窗口之间首尾相接，不会重叠，同一个元素不会同时属于多个窗口
            需要传递的参数:窗口大小
        滑动窗口
            特点:窗口和窗口会重叠，同一个元素同时属于多个窗口
            需要传递的参数:窗口大小、滑动步长
        会话窗口
            特点:窗口和窗口不会重叠，同一个元素不会同时属于多个窗口，窗口和窗口之间应该有一个间隙(Gap)
            需要传递的参数:时间间隔
        全局窗口
            特点：将数据放到同一个窗口中，默认是没有结束的，所以在使用全局窗口的时候，需要自定义触发器
            计数窗口的底层就是通过全局窗口实现的

窗口API
    开窗前是否进行了keyBy操作
        没有keyBy，针对整条流进行开窗，相当于并行设置为1
            wsDS.windowAll()
            wsDS.countWindowAll()
        keyBy,针对keyBy之后的每一组进行单独开窗，窗口之间相互不影响
            wsDS.keyBy().window()
            wsDS.keyBy().countWindow()
    窗口分配器---开什么样的窗口
       滚动处理时间窗口
            .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
       滑动处理时间窗口
            .window(SlidingProcessingTimeWindows.of(Time.seconds(10),Time.seconds(2)))
       处理时间会话窗口
            .window(ProcessingTimeSessionWindows.withGap(Time.seconds(10)))
       滚动事件时间窗口
            .window(TumblingEventTimeWindows.of(Time.seconds(10)))
       滑动事件时间窗口
            .window(SlidingEventTimeWindows.of(Time.seconds(10),Time.seconds(2)))
       事件时间会话窗口
            .window(EventTimeSessionWindows.withGap(Time.seconds(10)))

       滚动计数窗口
            .countWindow(10)
       滑动计数窗口
            .countWindow(10,2)

       全局窗口
            .window(GlobalWindows.create())
    窗口处理函数---如何处理窗口中的元素
        增量处理函数---窗口中来一条数据处理一次，不会对数据进行缓存
            优点：不会缓存数据，省空间
            缺点：获取不到窗口更详细的信息
            .reduce()
                窗口中元素的类型以及向下游传递的类型是一致的
                如果窗口中只有一个元素，reduce方法不会被执行
                reduce(value1,value2)
                    value1:中间累加结果
                    value2:新来的数据

            .aggregate()
                窗口中元素的类型、累加器的类型以及向下游传递的数据的类型可以不一致
                createAccumulator:属于当前窗口的第一个元素进来的时候执行
                add:窗口中每来一条数据都会被执行一次
                getResult: 窗口触发计算的时候执行一次


        全量处理函数---会将当前窗口的数据全部缓存下来，等窗口触发计算的时候再进行处理
            优点：可以获取窗口更详细的信息
            缺点：会将窗口的数据进行缓存，比较占用空间
            .apply()
                apply(String key, TimeWindow window, Iterable<WaterSensor> input, Collector<String> out)
            .process()
                process(String key, Context context, Iterable<WaterSensor> elements, Collector<String> out)
                process更底层，通过context对象除了可以获取窗口对象之外，还可以获取更丰富的信息

        在实际使用的过程中，可以增量 + 全量
            reduce + apply
            reduce + process
            aggregate + apply
            aggregate + process

    窗口触发器---何时触发窗口的计算
        .trigger()
    窗口移除器---在窗口触发计算后，在处理函数执行前或者后执行一些移除操作
        .evictor()

以滚动处理时间窗口为例
    什么时候创建窗口对象
        当有属于这个窗口的第一个元素到来的时候，创建窗口对象
    窗口对象是左闭右开的[ )
    窗口对象的起始和结束时间
        起始时间：向下取整
            final long remainder = (timestamp - offset) % windowSize;
            // handle both positive and negative cases
            if (remainder < 0) {
                return timestamp - (remainder + windowSize);
            } else {
                return timestamp - remainder;
            }
        结束时间： 起始时间 + 窗口大小
        最大时间： 结束时间 - 1ms

    窗口如何触发计算
        当系统时间到了窗口的最大时间的时候触发计算












