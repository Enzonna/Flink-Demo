DataStreamAPI
    环境准备
        创建本地环境
            StreamExecutionEnvironment.createLocalEnvironment();
        创建本地环境带WEBUI
            StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration())
            需要在pom.xml文件中提前添加flink-runtime-web依赖
        创建远程环境
            StreamExecutionEnvironment.createRemoteEnvironment(远程服务器ip,端口号,jar)
        根据实际执行场景自动创建环境
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()
            ->如果需要自动创建本地带webUI的执行环境，需要传递Configuration，并显式指定端口号

        关于运行模式设置
            STREAMING
            BATCH
            AUTOMATIC
    源算子-Source
        从集合中读取数据
            env.fromCollection(集合)
        从指定的元素中读取数据
            env.fromElements(元素...)
        从文件中读取数据
            env.readTextFile()  已过时
            文件连接器
                 FileSource<String> source = FileSource
                                .forRecordStreamFormat(new TextLineInputFormat(), new Path("D:\\dev\\workspace\\bigdata-0318\\input\\words.txt"))
                                .build();
                 env.fromSource(数据源对象,WaterMark,数据源名)

        Flink1.12前
            env.addSource(SourceFunction)
        Flink1.12后
            env.fromSource(Source)
        从kafka主题中读取数据
            KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                            //设置kafka集群的地址
                            .setBootstrapServers("hadoop102:9092")
                            //设置消费的主题
                            .setTopics("first")
                            //设置消费者组
                            .setGroupId("test")
                            //设置消费位点
                            //作为消费者，如何保证消费的精准一次:手动维护偏移量
                                KafkaSource->KafkaSourceReader->SortedMap<Long, Map<TopicPartition, OffsetAndMetadata>>
                            // 从最早位点开始消费
                            //.setStartingOffsets(OffsetsInitializer.earliest())
                            // 从最末尾位点开始消费
                            .setStartingOffsets(OffsetsInitializer.latest())
                            // 从时间戳大于等于指定时间戳（毫秒）的数据开始消费
                            //.setStartingOffsets(OffsetsInitializer.timestamp(1657256176000L))
                            // 从消费组提交的位点开始消费，不指定位点重置策略
                            //.setStartingOffsets(OffsetsInitializer.committedOffsets())
                            // 从消费组提交的位点开始消费，如果提交位点不存在，使用最早位点
                            //.setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                            //设置反序列化器
                            .setValueOnlyDeserializer(new SimpleStringSchema())
                            .build();

    转换算子-Transform
    输出算子-Sink
    提交作业
        同步提交作业 env.execute()
        异步提交作业 env.executeAsync()
        以异步提交作业为例，如果一个应用App下面有多个作业Job，部署到Yarn的不同模式下效果
            会话-session
                先启动集群，多个job都部署到同一个集群中，共享集群资源
                作业停止，不会对集群产生影响
            单作业-perjob
                不需要先启动集群，给每一个作业启动一个集群
                如果停止某一个作业，当前作业对应的集群也会停止，对其它的作业对应的集群没有影响
            应用-application
                不需要先启动集群，给每一个应用启动一个集群
                同一个应用下的多个作业，共享这一个集群
                如果停止某一个作业，整个集群停止，对其它的作业也会有影响


