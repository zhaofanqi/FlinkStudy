package window;

import entity.Click;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * ClassName TopUrl
 *
 * @Auther: 赵繁旗
 * @Date: 2022/4/24 14:14
 * @Description:  返回 一定时间内访问次数最多的URL
 *          TODO : top n 没能实现
 */
public class TopUrl {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Click> clickDataStreamSource = env.addSource(new CreateClicks());

        SingleOutputStreamOperator<Click> clickwithWaterMark = clickDataStreamSource.assignTimestampsAndWatermarks
                (WatermarkStrategy
                        .<Click>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Click>() {
                            @Override
                            public long extractTimestamp(Click element, long recordTimestamp) {
                                return element.timeStamp;
                            }
                        }));

        clickwithWaterMark.print("data");

        SingleOutputStreamOperator<Tuple2<String, Integer>> urlWithCount = clickwithWaterMark.keyBy(data -> data.url)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
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
                }, new ProcessWindowFunction<Integer, Tuple2<String, Integer>, String, TimeWindow>() {
                    @Override
                    public void process(String url, Context context, Iterable<Integer> elements, Collector<Tuple2<String, Integer>> out) throws Exception {
                        Timestamp start = new Timestamp(context.window().getStart());
                        Timestamp end = new Timestamp(context.window().getEnd());
                        String urlAndcount = url + "\t" + elements.iterator().next();
                        System.out.println("采集时间为：" + start + " ~ " + end + urlAndcount);
                        out.collect(Tuple2.of(url, elements.iterator().next()));
                    }
                });

        //urlWithCount 中存储的是每个url对应的访问次数
        SingleOutputStreamOperator<Tuple2<String, Integer>> top1 = urlWithCount.flatMap(new RichFlatMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
            Tuple2<String, Integer> initTuple2;

            @Override
            public void open(Configuration parameters) throws Exception {
                initTuple2 = Tuple2.of("", 0);
                super.open(parameters);
            }

            @Override
            public void close() throws Exception {

                super.close();
            }

            @Override
            public void flatMap(Tuple2<String, Integer> value, Collector<Tuple2<String, Integer>> out) throws Exception {
                if (value.f1 > initTuple2.f1) {
                    initTuple2 = value;
                }
                out.collect(initTuple2);
            }
        });
        top1.print("top1");

        env.execute();

    }
}
