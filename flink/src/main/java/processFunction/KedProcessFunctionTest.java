package processFunction;

import entity.Click;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import utils.CreateClicks;

import java.sql.Timestamp;
import java.util.concurrent.TimeUnit;

/**
 * ClassName KedProcessFunctionTest
 *
 * @Auther: 赵繁旗
 * @Date: 2022/4/25 14:32
 * @Description:
 */
public class KedProcessFunctionTest {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Click> clickDataStreamSource = env.addSource(new CreateClicks());

        SingleOutputStreamOperator<Click> clickwithWaterMark = clickDataStreamSource.assignTimestampsAndWatermarks(WatermarkStrategy
                .<Click>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<Click>() {
                    @Override
                    public long extractTimestamp(Click element, long recordTimestamp) {
                        return element.timeStamp;
                    }
                })
        );

        clickwithWaterMark.print("waterMark");
        SingleOutputStreamOperator<String> processResult = clickwithWaterMark.keyBy(data -> data.user)
                .process(new KeyedProcessFunction<String, Click, String>() {
                    @Override
                    public void processElement(Click value, Context ctx, Collector<String> out) throws Exception {

                        Timestamp watermarkTS = new Timestamp(ctx.timerService().currentWatermark());

                        Timestamp processTS = new Timestamp(ctx.timerService().currentProcessingTime());
                        //设置十秒的定时器
                        ctx.timerService().registerEventTimeTimer(ctx.timerService().currentWatermark()+10*1000L);
                        out.collect("收集的数据为"+value.user+"\t水位线为："+watermarkTS+"\t 处理时间为："+processTS+"事件事件"+value.timeStamp);
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {

                        out.collect(ctx.getCurrentKey()+"用户触发了定时器了。"+"定时器获取到的时间为:"+new Timestamp(timestamp));

                        super.onTimer(timestamp, ctx, out);
                    }
                });

        processResult.print("processResult");
        env.execute();

    }
}
