package window;

import entity.Click;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.Random;

/**
 * ClassName EventTimeWindowCount
 *
 * @Auther: 赵繁旗
 * @Date: 2022/4/22 16:19
 * @Description:        如果是事件时间窗口，则需要设置水位线
 *
 */
public class EventTimeWindowCount {


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        DataStreamSource<Click> clickSource = env.addSource(new SourceFunction<Click>() {
            String[] users = {"zhaofq", "taoxp", "zhangxy", "jj"};
            String urls[] = {"/home", "/cart", "/prod"};
            Boolean running = true;
            Random random = new Random();

            @Override
            public void run(SourceContext<Click> ctx) throws Exception {
                while (running) {
                    ctx.collect(new Click(users[random.nextInt(users.length)], urls[random.nextInt(urls.length)], System.currentTimeMillis()));
                    Thread.sleep(1000L);
                }
            }

            @Override
            public void cancel() {
                running = false;
            }
        });
        SingleOutputStreamOperator<Click> clickSourceWithWatermark = clickSource.assignTimestampsAndWatermarks(WatermarkStrategy
                .<Click>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<Click>() {
                    @Override
                    public long extractTimestamp(Click element, long recordTimestamp) {
                        return element.timeStamp;
                    }
                }));


        SingleOutputStreamOperator<Tuple2<String, Long>> result = clickSourceWithWatermark.map(new MapFunction<Click, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(Click value) throws Exception {
                return Tuple2.of(value.user, 1L);
            }
        }).keyBy(data -> data.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                        return Tuple2.of(value1.f0, value1.f1 + value2.f1);
                    }
                });

        result.print();
        env.execute();

    }

}
