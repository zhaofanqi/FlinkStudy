package window;

import entity.Click;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;
import org.apache.flink.util.OutputTag;
import utils.CreateClicks;

/**
 * ClassName OtherWindowFunction
 *
 * @Auther: 赵繁旗
 * @Date: 2022/4/24 20:29
 * @Description:    Flink   延迟处理三层保障： 水位线（一般很小）
 *                                          窗口延时关闭（一般）
 *                                          侧输入流
 *                                          正常时间窗口：a,A  水位线延迟：B  窗口延迟：C
 *                                    窗口什么时候关闭：水位线到达窗口延时时间时关闭（A+B+C）；
 *                                                   a<时间<A 放入窗口桶
 *                                            保障一：A<时间<A+B 放入窗口桶
 *                                            保障二：A+B<时间<A+B+C 放入窗口桶并触发计算
 *                                            保障三：A+B+C<时间 放入侧输入流
 *                                                  当数据已经小于指定窗口水位线但没有达到延迟关闭时间，则此时触发计算
 */
public class OtherWindowFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Click> clickDataStreamSource = env.addSource(new CreateClicks());

        SingleOutputStreamOperator<Click> clickwithWaterMark = clickDataStreamSource.assignTimestampsAndWatermarks(WatermarkStrategy.<Click>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<Click>() {
            @Override
            public long extractTimestamp(Click element, long recordTimestamp) {
                return element.timeStamp;
            }
        }));
/**
 *  Noted:  以下三种方式都是在开窗之后，窗口函数执行之前的方式
 *          触发器；
 *          移除器: evictBefore()
 *                 evictAfter()
 *          侧输入流：
 *                  生成指定id的侧输出操作 OutputTag<Click> sideLate = new OutputTag<>("late");
 *                 SingleOutputStreamOperator<Integer> clickSource =   stream.window()
 *                                                         .allowedLateness(Time.seconds(5))
 *                                                         .sideOutputLateData(sideLate)
 *                                                         .aggregate(windowFunction);
 *                 DataStream<Click> sideOutput = clickSource.getSideOutput(sideLate);// 此处得到的是侧输出流
 */

        OutputTag<Click> sideLate = new OutputTag<Click>("late"){};
        SingleOutputStreamOperator<Integer> clickSource = clickwithWaterMark.keyBy(data -> true)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
               .trigger(new Trigger<Click, TimeWindow>() {
                   @Override
                   public TriggerResult onElement(Click element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
                       return null;
                   }

                   @Override
                   public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
                       return null;
                   }

                   @Override
                   public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
                       return null;
                   }

                   @Override
                   public void clear(TimeWindow window, TriggerContext ctx) throws Exception {

                   }
               })
                .evictor(new Evictor<Click, TimeWindow>() {
                    //Called before windowing function.
                    @Override
                    public void evictBefore(Iterable<TimestampedValue<Click>> elements, int size, TimeWindow window, EvictorContext evictorContext) {
                    }

                    //Called after windowing function.
                    @Override
                    public void evictAfter(Iterable<TimestampedValue<Click>> elements, int size, TimeWindow window, EvictorContext evictorContext) {
                    }
                })
                .allowedLateness(Time.seconds(5))
                .sideOutputLateData(sideLate)
                .aggregate(new AggregateFunction<Click, Integer, Integer>() {
                    @Override
                    public Integer createAccumulator() {
                        return 0;
                    }

                    @Override
                    public Integer add(Click value, Integer accumulator) {
                        return accumulator + 1;
                    }

                    @Override
                    public Integer getResult(Integer accumulator) {
                        return accumulator;
                    }

                    @Override
                    public Integer merge(Integer a, Integer b) {
                        return null;
                    }
                });
        DataStream<Click> sideOutput = clickSource.getSideOutput(sideLate);// 此处得到的是侧输出流


        env.execute();

    }
}
