
处理函数
    .process(处理函数)
    处理函数可以获取上下文对象。通过上下文对象可以操作侧输出流、可以获取TimeService
处理函数分类
    ProcessFunction
        最基本的处理函数，基于DataStream直接调用.process()时作为参数传入。
    KeyedProcessFunction
        对流按键分组后的处理函数，基于KeyedStream调用.process()时作为参数传入。要想使用定时器，必须基于KeyedStream。
    ProcessWindowFunction
        开窗之后的处理函数，也是全窗口函数的代表。基于WindowedStream调用.process()时作为参数传入。
    ProcessAllWindowFunction
        同样是开窗之后的处理函数，基于AllWindowedStream调用.process()时作为参数传入。
    CoProcessFunction
        合并（connect）两条流之后的处理函数，基于ConnectedStreams调用.process()时作为参数传入。关于流的连接合并操作，我们会在后续章节详细介绍。
    ProcessJoinFunction
        间隔连接（interval join）两条流之后的处理函数，基于IntervalJoined调用.process()时作为参数传入。
    BroadcastProcessFunction
        广播连接流处理函数，基于BroadcastConnectedStream调用.process()时作为参数传入。这里的“广播连接流”BroadcastConnectedStream，是一个未keyBy的普通DataStream与一个广播流（BroadcastStream）做连接（conncet）之后的产物。关于广播流的相关操作，我们会在后续章节详细介绍。
    KeyedBroadcastProcessFunction
        按键分组的广播连接流处理函数，同样是基于BroadcastConnectedStream调用.process()时作为参数传入。
定时器
    当处理时间或者事件时间到了定时器指定的时间点的时候
    定时器会触发执行---onTimer
    注意：只有KeyedProcessFunction中才能使用定时器
