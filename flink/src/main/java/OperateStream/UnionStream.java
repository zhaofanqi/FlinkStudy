package OperateStream;

import entity.Click;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
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
import java.time.Duration;

/**
 * ClassName UnionStream
 *
 * @Auther: 赵繁旗
 * @Date: 2022/4/27 08:31
 * @Description: 合并流： 流的数据类型 一致
 * 2条流的水位线可能不同，合并流后，对外统一输出水位线（上游水位线中最小值）
 */
public class UnionStream {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Click> clickSource1 = env.addSource(new CreateClicks());
        DataStreamSource<Click> clickSource2 = env.addSource(new CreateClicks());
        clickSource1.print("source1");
        clickSource2.print("source2");

        SingleOutputStreamOperator<Click> click1WM = clickSource1.assignTimestampsAndWatermarks(WatermarkStrategy
                .<Click>forBoundedOutOfOrderness(Duration.ofSeconds(5))// 延迟5秒关闭窗户
                .withTimestampAssigner(new SerializableTimestampAssigner<Click>() {
                    @Override
                    public long extractTimestamp(Click element, long recordTimestamp) {
                        return element.timeStamp;
                    }
                }));

        SingleOutputStreamOperator<Click> click2WM = clickSource2.assignTimestampsAndWatermarks(WatermarkStrategy
                .<Click>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                .withTimestampAssigner(new SerializableTimestampAssigner<Click>() {
                    @Override
                    public long extractTimestamp(Click element, long recordTimestamp) {
                        return element.timeStamp;
                    }
                }));
        DataStream<Click> unionStream = click1WM.union(click2WM);

        SingleOutputStreamOperator<String> unionStreamCount = unionStream.keyBy(data -> true)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new ProcessWindowFunction<Click, String, Boolean, TimeWindow>() {
                    @Override
                    public void process(Boolean aBoolean, Context context, Iterable<Click> elements, Collector<String> out) throws Exception {
                        Timestamp wm = new Timestamp(context.currentWatermark());
                        Timestamp start = new Timestamp(context.window().getStart());
                        StringBuilder sb = new StringBuilder();
                        sb.append("===========窗口统计信息============" + "\t" + start + "\n");
                        sb.append("===========\t水位线为：" + wm + "\t==============\n");
                        for (Click element : elements) {
                            sb.append(element);
                            sb.append("\n");
                        }
                        Timestamp end = new Timestamp(context.window().getEnd());
                        sb.append("===========窗口统计信息============" + "\t" + end + "\n");
                        out.collect(sb.toString());
                    }
                });

        unionStreamCount.print("合并流输出");
        env.execute();

    }
}
