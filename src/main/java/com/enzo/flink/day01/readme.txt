
Flink
    是一个框架
    是一个分布式处理引擎
    对有界或者无界流数据进行实时计算

Flink和Sparkstreaming对比
                          Flink                       Sparkstreaming
    时间语义           事件时间、处理时间                   处理时间
    窗口               灵活                               不够灵活
    状态               有状态                             无状态(updateStateByKey)
    容错               检查点                             弱
    动态SQL            支持                               不支持

Flink的分层API
    SQL
    TableAPI
    DataStreamAPI|DataSetAPI(Flink1.12后，基本不用)
    处理函数 process

    越顶层，使用越简单
    越底层，使用越灵活(可以处理较复杂的业务)

通过WordCount对比DataStreamAPI和DataSetAPI
                        DataStreamAPI                                  DataSetAPI
    准备环境      StreamExecutionEnvironment                        ExecutionEnvironment
    分组         keyBy                                              groupBy
    作业提交      env.execute()                                     自动提交
    处理方式      流中每来一条数据都会对其进行处理                       收集齐所有数据后，处理一次

Flink运行模式(指定Flink程序在什么地方执行)
    Standalone、k8s、mesos、Yarn...

Flink集群中的角色
    客户端
    JobManager
    TaskManager

Flink部署模式
    会话模式-session
        需要先启动集群
        多个job共享集群资源，作业在共享资源的时候，相互之间会有影响
        当作业取消的时候，对集群是没有影响的

    单作业模式-per-job
        不需要先启动集群
        当作业提交的时候，会给每一个作业单独启动一个集群
        当作业取消的时候，集群也会停掉

    应用模式-application
        不需要先启动集群
        当应用提交的时候，会给每一个应用(一个应用下(app)，可能会有多个作业(job))单独启动一个集群
        当作业取消的时候，集群也会停掉

    如果是会话模式以及单作业模式，在客户端会执行一些转换操作，客户端压力比较大
    如果是应用模式，转换操作会到服务器(JobManager)上执行，客户端压力变小

    在开发的时候，首选应用模式

Flink On Yarn
    Yarn会话
        启动集群            bin/yarn-session.sh
        WEBUI提交作业
        命令行提交作业       bin/flink run -d -c 类的全限定名 jar包路径
    Yarn-Per-Job
        不需要启动集群
        只能通过命令行提交作业     bin/flink run -d -t yarn-per-job -c 类的全限定名 jar包路径

    YarnApplication
        不需要启动集群
        只能通过命令行提交作业     bin/flink run-application -d -t yarn-application -c 类的全限定名 jar包路径



