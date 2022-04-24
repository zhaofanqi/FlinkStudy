package window;

import entity.Click;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import utils.CreateClicks;

import java.sql.Timestamp;
import java.util.HashSet;

/**
 * ClassName ProcessFunAndAggFuncition
 *
 * @Auther: 赵繁旗
 * @Date: 2022/4/24 11:09
 * @Description:        将 AggregateFunction & ProcessWindowFunction 结合
 *                          前者计算，后者输出相关环境信息，同时前者的输出作为后者的输入
 */
public class ProcessFunAndAggFuncition {
    public static void main(String[] args) throws Exception {

        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        // 输入数据源
        DataStreamSource<Click> clickSource = env.addSource(new CreateClicks());
        // 数据增加水位线

        clickSource.print("data :");
        SingleOutputStreamOperator<Click> clickwithWaterMark = clickSource.assignTimestampsAndWatermarks(WatermarkStrategy.
                <Click>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<Click>() {
                    @Override
                    public long extractTimestamp(Click element, long recordTimestamp) {
                        return element.timeStamp;
                    }
                }));

        // 转换： 将输入的数据源，转换为：一句话输出 uv 和 PV 统计结果
        SingleOutputStreamOperator<String> result = clickwithWaterMark.keyBy(data -> true)// 此处的分区字段影响ProcessWindowFunction中的参数三
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .aggregate(new Aggre(), new ProcessWindowFunction<Tuple2<Integer, Integer>, String, Boolean, TimeWindow>() {
                    @Override
                    public void process(Boolean a, Context context, Iterable<Tuple2<Integer, Integer>> elements, Collector<String> out) throws Exception {
                        for (Tuple2<Integer, Integer> element : elements) {
                            System.out.println("访问的次数为： " + element.f0 + "； 访问的人数为： " + element.f1);
                        }
                        Timestamp start = new Timestamp(context.window().getStart());
                        Timestamp end = new Timestamp(context.window().getEnd());
                        Timestamp waterMark = new Timestamp(context.currentWatermark());
                        Integer pv = elements.iterator().next().f0;
                        Integer uv = elements.iterator().next().f1;

                        // 将每一个用户的结果都进行输出了
                        out.collect("收集数据的时间范围为 ：" + start + " ~ " + end + "访问的次数为：" + pv + "访问的人数为：" + uv+"水位线为： "+waterMark);

                    }
                });
        result.print("result: ");



        // 程序挂起
        env.execute();

    }
    public  static class Aggre implements AggregateFunction<Click, Tuple2<Integer,HashSet<String>>, Tuple2<Integer,Integer>>{

        @Override
        public Tuple2<Integer, HashSet<String>> createAccumulator() {
            HashSet<String> set = new HashSet<>();
            return Tuple2.of(0,set);
        }

        // 中间结果保存 访问次数   访问的总人数
        @Override
        public Tuple2<Integer, HashSet<String>> add(Click value, Tuple2<Integer, HashSet<String>> accumulator) {
            accumulator.f1.add(value.user);
            accumulator.f0+=1;
            return accumulator;
        }

        @Override
        public Tuple2<Integer, Integer> getResult(Tuple2<Integer, HashSet<String>> accumulator) {
            return Tuple2.of(accumulator.f0,accumulator.f1.size());
        }

        @Override
        public Tuple2<Integer, HashSet<String>> merge(Tuple2<Integer, HashSet<String>> a, Tuple2<Integer, HashSet<String>> b) {
            return null;
        }
    }
    
    
    
}
