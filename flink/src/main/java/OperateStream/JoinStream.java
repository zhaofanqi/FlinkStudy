package OperateStream;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.lang.reflect.Type;
import java.time.Duration;

/**
 * ClassName JoinStream
 *
 * @Auther: 赵繁旗
 * @Date: 2022/4/27 14:35
 * @Description:    按照指定key连接流
 *          案例：客户给第三方平台支付，第三方平台给商家反馈支付成功，商家提醒客户是否购买成功
 */
public class JoinStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Tuple3<String, String, Long>> appSource = env.fromElements(
                Tuple3.of("order-1", "card", 1000L),
                Tuple3.of("order-2", "pen", 1000L),
                Tuple3.of("order-3", "notebook", 1000L)
        );
        SingleOutputStreamOperator<Tuple3<String, String, Long>> appSourceWithWM = appSource.assignTimestampsAndWatermarks(WatermarkStrategy
                .<Tuple3<String, String, Long>>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                    @Override
                    public long extractTimestamp(Tuple3<String, String, Long> element, long recordTimestamp) {
                        return element.f2;
                    }
                }));

        DataStreamSource<Tuple4<String, String,Integer, Long>> thirdPartSource = env.fromElements(
                Tuple4.of("order-1", "success", 100,4000L),
                Tuple4.of("order-4", "success", 100,5000L),
                Tuple4.of("order-2", "success", 200,3000L)

        );
        SingleOutputStreamOperator<Tuple4<String, String,Integer, Long>> thirdPartSourceWithWM = thirdPartSource.assignTimestampsAndWatermarks(WatermarkStrategy
                .<Tuple4<String, String,Integer, Long>>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple4<String, String,Integer, Long>>() {
                    @Override
                    public long extractTimestamp(Tuple4<String, String,Integer, Long> element, long recordTimestamp) {
                        return element.f3;
                    }
                }));
        appSourceWithWM.connect(thirdPartSourceWithWM)
                .keyBy(data -> data.f0, data -> data.f0)
                //因为需要匹配，从而可以定义状态，open()中实现 数据的到达存在先后，因为设置定时器，校验是否另外一个流中有记录
                .process(new CoProcessFunction<Tuple3<String,String,Long>, Tuple4<String,String,Integer,Long>, String>() {

                    //使用valueState保存

                    ValueState<Tuple3<String,String,Long>> appValueState;
                    ValueState<Tuple4<String,String,Integer,Long>> thirdPartValueState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        //Tuple3<String,String,Long>
                        appValueState=  getRuntimeContext().getState(new ValueStateDescriptor<Tuple3<String,String,Long>>("app-state", Types.TUPLE(Types.STRING,Types.STRING,Types.LONG)));
                        thirdPartValueState=  getRuntimeContext().getState(new ValueStateDescriptor<Tuple4<String,String,Integer,Long>>("thirdpart-state", Types.TUPLE(Types.STRING,Types.STRING,Types.INT,Types.LONG)));
                        super.open(parameters);

                    }

                    @Override
                    public void processElement1(Tuple3<String, String, Long> value, Context ctx, Collector<String> out) throws Exception {
                        // 进入该方法说明 appValueState 中已经有了，那么就直接判断 thirdPartValueState 是否有对应的状态

                        if (thirdPartValueState.value()!=null){
                            out.collect("app-state 在thirdpart-state中匹配上"+value);
                            appValueState.clear();// 说明已经有了，则此时不需要继续保存状态
                            thirdPartValueState.clear();
                        }else {
                            appValueState.update(value);
                            ctx.timerService().registerEventTimeTimer(value.f2+5000);//由于此处限定时间一定是几秒内到达，此时使用interval join 会更好用
                        }


                    }

                    @Override
                    public void processElement2(Tuple4<String, String, Integer, Long> value, Context ctx, Collector<String> out) throws Exception {
                        // 进入该方法说明 thirdPartValueState 中已经有了，那么就直接判断 appValueState 是否有对应的状态

                        if (appValueState.value()!=null){
                            out.collect("thirdpart-state 在 app-state中匹配上"+value);
                            thirdPartValueState.clear();// 说明已经有了，则此时不需要继续保存状态
                            appValueState.clear();
                        }else {
                            thirdPartValueState.update(value);
                            ctx.timerService().registerEventTimeTimer(value.f2+4000);//因为第三方平台可能会一慢点，所以此时的定时可稍慢一些
                        }
                    }


                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        super.onTimer(timestamp, ctx, out);

                        // 因为是分组了，所以该分组下只要有一个还不为空，说明没匹配上
                        if (appValueState.value()!=null){
                            out.collect("app中没有匹配成功"+appValueState.value());
                        }
                        if (thirdPartValueState.value()!=null){
                            out.collect("third没有匹配成功"+thirdPartValueState.value());
                        }


                    }
                }).print("final");



        env.execute();
    }
}
