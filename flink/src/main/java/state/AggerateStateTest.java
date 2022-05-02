package state;

import entity.Click;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import utils.CreateClicks;

import java.io.IOException;
import java.time.Duration;

/**
 * ClassName AggerateStateTest
 *
 * @Auther: 赵繁旗
 * @Date: 2022/5/1 08:34
 * @Description:    使用一般规约函数，查看用户点击页面时间间隔
 */
public class AggerateStateTest {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Click> clickSource = env.addSource(new CreateClicks());

        SingleOutputStreamOperator<Click> clickWM = clickSource.assignTimestampsAndWatermarks(WatermarkStrategy
                .<Click>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<Click>() {
                    @Override
                    public long extractTimestamp(Click element, long recordTimestamp) {
                        return element.timeStamp;
                    }
                }));

        clickWM.print();
        // 中间结果保存： 规约函数：次数，每次访问时间间隔总和，
        //          ValueState 记录上一次访问的
        SingleOutputStreamOperator<String> aggregateResult = clickWM.keyBy(data -> data.user)
                .process(new KeyedProcessFunction<String, Click, String>() {
                    AggregatingState<Click,Tuple2<Long,Long>> aggregatingState;// 输入与输出
                    ValueState<Long> valueState;
                    ValueState<Integer> countState;

                    @Override
                    public void open(Configuration parameters) throws Exception {


                        valueState=getRuntimeContext().getState(new ValueStateDescriptor<Long>("value state",Types.LONG));
                        countState=getRuntimeContext().getState(new ValueStateDescriptor<Integer>("count state",Types.INT));


                        aggregatingState = getRuntimeContext().getAggregatingState(new AggregatingStateDescriptor<Click, Tuple2<Long,Long>, Tuple2<Long,Long>>("aggre"
                                ,
                                new AggregateFunction<Click, Tuple2<Long, Long>, Tuple2<Long, Long>>() {
                                    @Override
                                    public Tuple2<Long, Long> createAccumulator() {
                                        return Tuple2.of(0L,0L);
                                    }

                                    @Override
                                    public Tuple2<Long, Long> add(Click value, Tuple2<Long, Long> accumulator) {


                                        long intervalTime= 0;
                                        try {
                                            Integer count = countState.value();
                                            if (count==null){
                                                countState.update(0);
                                            }else {
                                                countState.update(count+1);
                                            }

                                            if (valueState.value()==null){
                                                valueState.update(value.timeStamp);
                                                return Tuple2.of(accumulator.f0+1,0L);
                                            }else {
                                                intervalTime = value.timeStamp-valueState.value();
                                                valueState.update(value.timeStamp);
                                                return Tuple2.of(accumulator.f0+1,accumulator.f1+intervalTime);
                                            }


                                        } catch (IOException e) {
                                            e.printStackTrace();
                                        }
                                      return Tuple2.of(0L,0L);
                                    }

                                    @Override
                                    public Tuple2<Long, Long> getResult(Tuple2<Long, Long> accumulator) {
                                        return accumulator;
                                    }

                                    @Override
                                    public Tuple2<Long, Long> merge(Tuple2<Long, Long> a, Tuple2<Long, Long> b) {
                                        return null;
                                    }
                                }
                                , Types.TUPLE(Types.LONG, Types.LONG))//ACC

                        );

                    }

                    @Override
                    public void processElement(Click value, Context ctx, Collector<String> out) throws Exception {

                        aggregatingState.add(value);
                        if (countState.value()!=null&&countState.value()%5==0){
                            Tuple2<Long, Long> countInterval = aggregatingState.get();
                            out.collect("用户："+ctx.getCurrentKey()+"\t次数为"+countInterval.f0+"\t时间间隔为："+countInterval.f1+"\t平均时间间隔为："+ (countInterval.f1/countInterval.f0));
                        }

                    }
                });

        aggregateResult.print();
        env.execute();

    }
}
