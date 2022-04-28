package state;

import akka.stream.impl.ReducerState;
import entity.Click;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.LongValue;
import org.apache.flink.util.Collector;
import utils.CreateClicks;

import java.time.Duration;
import java.util.Iterator;

/**
 * ClassName StateTest
 *
 * @Auther: 赵繁旗
 * @Date: 2022/4/28 14:20
 * @Description: 状态：算子计算需要不仅仅依赖当前的数据，还需要之前的记录做支持
 * ：原始状态：自已定义，自己处理
 * ：托管状态（交由flink处理）
 * Operator state
 * Keyed state
 * 状态操作执行需要keyby
 * Valuestate  仅保存一个值
 * ListState   保存list
 * MapState    保存Map
 * ReducingState 规约，将前面的值与当前值进行计算(输入输出类型相同)
 * AggregateState 规约，将前面的值与当前值进行计算(输入输出类型可以不同)
 */
public class StateTest {
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

        SingleOutputStreamOperator<String> stateResult = clickWM.keyBy(data -> data.user)
                .flatMap(new RichFlatMapFunction<Click, String>() {
                    //定义测试的状态
                    ValueState<Long> valueState;
                    ListState<Click> listState;// 记录数据
                    MapState<String, Long> mapState;// 计数
                    ReducingState<Long> reducingState;// 计数
                    AggregatingState<Click, Long> aggregatingState;// 计数

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        // 状态初始化
                        valueState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("value-state", Types.LONG));
                        listState = getRuntimeContext().getListState(new ListStateDescriptor<Click>("list-state", Click.class));
                        mapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Long>("map-state", Types.STRING, Types.LONG));
                        reducingState = getRuntimeContext().getReducingState(new ReducingStateDescriptor<Long>("reduce-state", new ReduceFunction<Long>() {

                            @Override
                            public Long reduce(Long value1, Long value2) throws Exception {
                                return value1/value1 + value2/value2;
                            }
                        }, Types.LONG));
                        aggregatingState = getRuntimeContext().getAggregatingState(new AggregatingStateDescriptor<Click, Long, Long>("aggre-state", new AggregateFunction<Click, Long, Long>() {
                            @Override
                            public Long createAccumulator() {
                                return 0L;
                            }

                            @Override
                            public Long add(Click value, Long accumulator) {
                                return accumulator + 1;
                            }

                            @Override
                            public Long getResult(Long accumulator) {
                                return accumulator;
                            }

                            @Override
                            public Long merge(Long a, Long b) {
                                return null;
                            }
                        }, Types.LONG));

                        super.open(parameters);
                    }

                    @Override
                    public void flatMap(Click value, Collector<String> out) throws Exception {
                        /*System.out.println(value.user+"\t"+valueState.value());
                        valueState.update(value.timeStamp);
                        System.out.println(value.user+"\t"+valueState.value());*/

                        listState.add(value);
                        mapState.put(value.user,mapState.get(value.user)==null?1:mapState.get(value.user)+1);

                        reducingState.add(reducingState.get()==null?1L:value.timeStamp);
                        aggregatingState.add(value);

                        Iterator<Click> iterator = listState.get().iterator();
                        while (iterator.hasNext()) {
                            System.out.println(iterator.next()+"\n");
                        }

                        System.out.println("map state :"+mapState.get(value.user));
                        System.out.println("reduce state :"+reducingState.get());
                        System.out.println("aggre state :"+aggregatingState.get());
                        System.out.println("============\n");

                    }
                });


        stateResult.print();
        env.execute();
    }
}
