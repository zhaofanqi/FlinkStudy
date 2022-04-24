package window;

import entity.Click;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;



import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * ClassName WindowCount
 *
 * @Auther: 赵繁旗
 * @Date: 2022/4/22 14:24
 * @Description:
 *              基于窗口进行实时统计。使用窗口之前要分区，否则并行度只有1 即 windowAll 将所有的数据放在一个窗口去执行（windowAll需要触发器去触发）
 *                  窗口大小 数据量的多少
 *                  窗口步长 统计的频率
 *              窗口按照时间划分规则分：事件时间 处理时间
 *                  窗口移动方式： 滚动  滑动   同时滚动也可以看为滑动步长为窗口大小的滑动窗口
 *
 *                  此处案例： 每秒生成数据，窗口10 滑动步长2 进行统计
 */
public class ProcessTimeWindowCount {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        DataStreamSource<Click> clickSource = env.addSource(new SourceFunction<Click>() {
            String[] users = {"zhaofq", "taoxp", "zhangxy", "duyx"};
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
        // 统计每个用户访问的数据量
        clickSource.map(new MapFunction<Click, Tuple2<String,Long>>() {
                    @Override
                    public Tuple2<String, Long> map(Click value) throws Exception {
                        return Tuple2.of(value.user,1L);
                    }
                }).keyBy(data->data.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))// 如果此处指定为事件时间 则需要设置水位线

                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                        return  Tuple2.of(value1.f0,value1.f1+value2.f1);
                    }
                })
                .print();
        env.execute();






    }
}
