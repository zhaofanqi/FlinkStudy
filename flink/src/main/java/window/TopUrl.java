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
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import utils.CreateClicks;

import java.sql.Timestamp;
import java.util.*;

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

        /**
         *  TOP N 的实现思路：开窗
         *                  aggregate ： 得到 url 访问次数  存入map[map里面可快速判读是否包含key]
         *                        将map结果存入 Arraylist  并进行排序「这一步是自己没想到的，还是Java 不够熟悉」
         *                  processFunction 将aggregate的输出结果整合 窗口信息对外输出
         *
          */
//SingleOutputStreamOperator<Tuple2<String, Integer>> urlWithCount =
        SingleOutputStreamOperator<String> topNResult = clickwithWaterMark
                .windowAll(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .aggregate(new AggregateFunction<Click, HashMap<String, Integer>, ArrayList<Tuple2<String, Integer>>>() {

                    @Override
                    public HashMap<String, Integer> createAccumulator() {
                        return new HashMap<String, Integer>();
//                        return null;
                    }

                    @Override
                    public HashMap<String, Integer> add(Click value, HashMap<String, Integer> accumulator) {
                        if (accumulator.containsKey(value.url)) {
                            Integer count = accumulator.get(value.url);
                            accumulator.put(value.url, count + 1);
                        } else {
                            accumulator.put(value.url, 1);
                        }
                        return accumulator;
                    }

                    // 已经得到所有统计结果，此时需要排序
                    @Override
                    public ArrayList<Tuple2<String, Integer>> getResult(HashMap<String, Integer> accumulator) {
                        ArrayList<Tuple2<String, Integer>> urlWithCount = new ArrayList<Tuple2<String, Integer>>();
                        for (String key : accumulator.keySet()) {
                            urlWithCount.add(Tuple2.of(key, accumulator.get(key)));
                        }
                        urlWithCount.sort(new Comparator<Tuple2<String, Integer>>() {
                            @Override
                            public int compare(Tuple2<String, Integer> o1, Tuple2<String, Integer> o2) {

                                return o2.f1 - o1.f1;
                            }
                        });

                        return urlWithCount;
                    }

                    @Override
                    public HashMap<String, Integer> merge(HashMap<String, Integer> a, HashMap<String, Integer> b) {
                        return null;
                    }
                }, new ProcessAllWindowFunction<ArrayList<Tuple2<String, Integer>>, String, TimeWindow>() {
                    @Override
                    public void process(Context context, Iterable<ArrayList<Tuple2<String, Integer>>> elements, Collector<String> out) throws Exception {
                        // 整理构建输出即可
                        ArrayList<Tuple2<String, Integer>> result = elements.iterator().next();
                        StringBuilder sb = new StringBuilder();
                        Timestamp tsEnd = new Timestamp(context.window().getEnd());
                        Timestamp tsStart = new Timestamp(context.window().getStart());
                        sb.append("=====窗口结果开始=====" + tsStart+"\t\n");
                        for (int i = 0; i < 2; i++) {
                            Tuple2<String, Integer> resultTuple2 = result.get(i);
                            sb.append("top " + (i+1) + "\t 的url为：" + resultTuple2.f0 + "\t 对应的访问次数为" + resultTuple2.f1+"\n");
                        }
                        sb.append("======窗口结束====" + tsEnd+"\t\n");
                        out.collect(sb.toString());

                    }
                });
        topNResult.print("topN");


        env.execute();

    }
}
