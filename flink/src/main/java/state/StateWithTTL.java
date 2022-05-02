package state;

import entity.Click;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import utils.CreateClicks;

/**
 * ClassName StateWithTTL
 *
 * @Auther: 赵繁旗
 * @Date: 2022/5/1 10:52
 * @Description:        算子状态还有存活时间
 */
public class StateWithTTL {
    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Click> clickSource = env.addSource(new CreateClicks());

        SingleOutputStreamOperator<String> processResult = clickSource.keyBy(data -> data.user)
                .process(new KeyedProcessFunction<String, Click, String>() {
                    ValueState<Long> valueState;


                    @Override
                    public void open(Configuration parameters) throws Exception {
                        StateTtlConfig build = StateTtlConfig.newBuilder(Time.hours(1))
                                .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)//ttl变更策略
                                .setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp)// 超时时，返回哪一个
                                .build();
                        ValueStateDescriptor<Long> valueStateDes = new ValueStateDescriptor<>("value", Types.LONG);
                        valueStateDes.enableTimeToLive(build);
                        valueState=getRuntimeContext().getState(valueStateDes);


                    }

                    @Override
                    public void processElement(Click value, Context ctx, Collector<String> out) throws Exception {

                    }



                });
        processResult.print();
        env.execute();

    }
}
