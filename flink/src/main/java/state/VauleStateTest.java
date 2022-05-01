package state;

import apple.laf.JRSUIState;
import entity.Click;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import utils.CreateClicks;

import java.time.Duration;

/**
 * ClassName VauleStateTest
 *
 * @Auther: 赵繁旗
 * @Date: 2022/4/28 17:14
 * @Description: 场景：按照用户周期性统计用户的点击数据。
 * 注意：此处不是基于窗口统计，而是基于历史数据周期性统计，最好是用户发生点击以后，10秒以后统计一次，如果没有发生点击则不统计
 * 解题思路：
 * 以用户分组
 * 由于是历史数据，而不是窗口，因此此处需要状态保存，而状态需要 getRuntimeContext()--> 采用processFunction或者富函数
 * 基于用户点击周期统计，因为需要以点击行为时间创建定时器，后续数据是否定义定时器需要进行判断.需要2个ValueState 一个是统计值，一个是定时器
 */
public class VauleStateTest {
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
        SingleOutputStreamOperator<String> valueStateResult = clickWM.keyBy(data -> data.user)
                .process(new KeyedProcessFunction<String, Click, String>() {
                    ValueState<Integer> countState;
                    ValueState<Long> timeState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        countState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("count", Integer.TYPE));
                        timeState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("time", Types.LONG));
                    }

                    // 注意此处没有执行
                    @Override
                    public void processElement(Click value, Context ctx, Collector<String> out) throws Exception {
                        if (timeState.value() == null) {
                            ctx.timerService().registerEventTimeTimer(value.timeStamp + 10 * 1000);
                            timeState.update(value.timeStamp + 10 * 1000);
                        }
                        countState.update(countState.value() == null ? 1 : countState.value() + 1);

                    }


                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        out.collect(ctx.getCurrentKey() + "的pv:\t" + countState.value());
                        timeState.clear();//定时器执行完要即使销毁
//                        countState.clear();// 如果清除统计结果，则为基于统计数据的窗口

                    }
                });
        valueStateResult.print("count");


        /*// 按照用户定时执行
        clickWM.keyBy(data->data.user).flatMap(new RichFlatMapFunction<Click, String>() {
            ValueState<Integer> countState;
            ValueState<Long> timeState;

            @Override
            public void open(Configuration parameters) throws Exception {
                countState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("count", Integer.TYPE));
                timeState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("time", Types.LONG));
            }

            // 确少定时任务服务

            @Override
            public void flatMap(Click value, Collector<String> out) throws Exception {
                countState.update(countState.value()==null?1:countState.value()+1);
                if (timeState.value()==null){
                    getRuntimeContext().
                }

            }



        });*/

        env.execute();

    }
}
