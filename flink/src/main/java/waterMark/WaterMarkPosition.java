package waterMark;

import entity.Click;
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
import window.ProcessFunAndAggFuncition;

import java.sql.Timestamp;
import java.util.HashSet;

/**
 * ClassName WaterMarkPosition
 *
 * @Auther: 赵繁旗
 * @Date: 2022/4/25 12:19
 * @Description:
 */
public class WaterMarkPosition {
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
                .aggregate(new Aggre(), new ProcessWindowFunction<String, String, Boolean, TimeWindow>() {
                    @Override
                    public void process(Boolean a, Context context, Iterable<String> elements, Collector<String> out) throws Exception {
                        Timestamp start = new Timestamp(context.window().getStart());
                        Timestamp end = new Timestamp(context.window().getEnd());
                        Timestamp waterMark = new Timestamp(context.currentWatermark());


                        // 将每一个用户的结果都进行输出了
                        out.collect("收集数据的时间范围为 ：" + start + " ~ " + end + "水位线"+waterMark+"：输出"+elements.iterator().next().toString());

                    }
                });
        result.print("result: ");



        // 程序挂起
        env.execute();

    }
    public  static class Aggre implements AggregateFunction<Click, String, String> {


        @Override
        public String createAccumulator() {
            return "start";
        }

        @Override
        public String add(Click value, String accumulator) {

            return accumulator+="\t"+value.user+"\t"+value.timeStamp+"\n";
        }

        @Override
        public String getResult(String accumulator) {
            return accumulator;
        }

        @Override
        public String merge(String a, String b) {
            return null;
        }
    }
}
