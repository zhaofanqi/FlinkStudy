package window;

import entity.Click;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.HashSet;
import java.util.Random;

/**
 * ClassName UVAndPVAggregateFunction
 *
 * @Auther: 赵繁旗
 * @Date: 2022/4/22 16:30
 * @Description:   使用 aggregateFunction 实现UV and PV 的统计
 * PV： 页面访问次数
 * UV： 用户访问次数
 * PV/UV 平均每个用户访问次数：用户活跃度
 */
public class UVAndPVAggregateFunction {
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
        clickSource.print();
        SingleOutputStreamOperator<Click> clickSourceWithWatermark = clickSource.assignTimestampsAndWatermarks(WatermarkStrategy
                .<Click>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<Click>() {
                    @Override
                    public long extractTimestamp(Click element, long recordTimestamp) {
                        return element.timeStamp;
                    }
                }));


        // 指定窗口内，用户的平均访问次数实现 user -> pv,uv
        SingleOutputStreamOperator<Tuple2<String, Long>> result = clickSourceWithWatermark.keyBy(data -> data.user)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                // in  acc out  acc(中间变量存储) out(输出数据格式)  此处中间结果保存：页面访问次数 访问的用户名set的size()
                .aggregate(new AggregateFunction<Click, Tuple2<Long, Integer>, Tuple2<String, Long>>() {
                    HashSet<Click> set = new HashSet<Click>();
                    @Override
                    public Tuple2<Long, Integer> createAccumulator() {

                        return Tuple2.of(0L,0);
                    }

                    @Override
                    public Tuple2<Long, Integer> add(Click value, Tuple2<Long, Integer> accumulator) {
                        set.add(value);

                        return Tuple2.of(accumulator.f0+1,set.size());

                    }

                    // 传入的是 访问次数，集合中的大小
                    @Override
                    public Tuple2<String, Long> getResult(Tuple2<Long, Integer> accumulator) {

                        return Tuple2.of(set.iterator().next().user,accumulator.f0);
                    }

                    @Override
                    public Tuple2<Long, Integer> merge(Tuple2<Long, Integer> a, Tuple2<Long, Integer> b) {
                        return null;
                    }


                });

        result.print();
        env.execute();




    }
}
