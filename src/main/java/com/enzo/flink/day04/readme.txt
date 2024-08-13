富函数
    在流中数据进行处理的时候，相关算子要求传递一个处理函数实体作为参数
        例如：map(MapFunction)、filter(FilterFunction)、flatMap(FlatMapFunction)
        默认参数声明的形式是接口类型，其实除了接口之外，每一个声明的函数类，都有一个对应的富函数(抽象类)实现
        富函数的形式 Rich +接口名   例如：   MapFunction -> RichMapFunction
        富函数和普通的接口比起来，多出了如下功能
            提供了上下文对象，可以通过上下文对象获取更丰富的信息
            提供了带生命周期的方法
            open
                方法初始化的时候执行的代码
                在每一个算子子任务(并行度)上，只执行一次
                用于进行连接的初始化
            close
                方法执行结束的时候执行的代码
                在每一个算子子任务(并行度)上，只执行一次
                用于资源的释放
                如果处理的是无界数据，close方法不会被执行
分区操作(算子)
    shuffle
    rebalance
    rescale
    broadcast
    global
    one-to-one --- forward
    keyBy---hash
    custom---custom
分流
    将一条流拆分为2条流或者多条流
    filter
        涉及对同一条流的数据处理多次，效率较低
    侧输出流
        首先定义侧输出流标签OutputTag
        在定义侧输出流标签的时候，存在泛型擦除
            可以通过匿名内部类的方式创建对象
            可以在创建对象的传递侧输出流中的数据类型
        注意：如果要向使用侧输出流，只能通过process算子对流中数据进行处理，因为在底层的processElement方法中，可以获取上下文对象

合流
    union
        可以合并2条或者多条流
        要求：参与合并的流中数据类型必须一致
    connect
        只能对2条流进行合并
        参与连接的两条流数据类型可以不一致


Flink读写外部系统的方式
    Flink1.12前
        env.addSource(SourceFunction)
        流.addSink(SinkFunction)
    从Flink1.12开始
        env.fromSource(Source)
        流.sinkTo(Sink)


Flink从kafka中读取数据
    创建KafkaSource
    env.fromSource(kafkaSource)
    KafkaSource可以保证读取的精准一次，KafkaSource->KafkaSourceReader->成员变量维护偏移量

Flink向kafka中写入数据，一致性如何保证(先了解)
    检查点必须要开启
    .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
    .setTransactionalIdPrefix("xxx")
    //检查点超时时间 < 事务超时时间 <= 事务最大超时时间(15min)
    .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,15*60*1000 + "")
    在消费端，设置消费的隔离级别read_committed
    .setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG,"read_committed")
