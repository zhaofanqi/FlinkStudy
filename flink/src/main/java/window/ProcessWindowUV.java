package window;

import entity.Click;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.TimeUtils;
import utils.CreateClicks;

import java.sql.Timestamp;
import java.util.HashSet;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * ClassName ProcessWindowUV
 *
 * @Auther: 赵繁旗
 * @Date: 2022/4/24 10:05
 * @Description: 使用全窗口统计函数去统计数据 uv 访问次数
 */
public class ProcessWindowUV {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Click> clickSource = env.addSource(new CreateClicks());
        clickSource.print("source");
        SingleOutputStreamOperator<Click> clickWaterMark = clickSource.assignTimestampsAndWatermarks(WatermarkStrategy
                .<Click>forMonotonousTimestamps()// 生成水位线生成器策略
                .withTimestampAssigner(new SerializableTimestampAssigner<Click>() {
                    @Override
                    public long extractTimestamp(Click element, long recordTimestamp) {
                        return element.timeStamp;
                    }
                })// 水位线时间字段的指定
        );
        SingleOutputStreamOperator<String> result = clickWaterMark.keyBy(data -> true)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new UVProcessWindowFunction());
        result.print("result");

        env.execute();

    }

    // 全窗口统计
    private static class UVProcessWindowFunction extends ProcessWindowFunction<Click,String,Boolean,TimeWindow>{
        @Override
        public void process(Boolean aBoolean, Context context, Iterable<Click> elements, Collector<String> out) throws Exception {
            HashSet<String> clicks = new HashSet<String>();
            for (Click element : elements) {
                clicks.add(element.user);
            }
            long start = context.window().getStart();
            long end = context.window().getEnd();

            out.collect("收集的时间范围为："+new Timestamp(start)+" ～ "+new Timestamp(end)+" 收集的用户数目为： "+clicks.size());

        }
    }
}
