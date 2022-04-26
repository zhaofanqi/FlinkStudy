package window;

import entity.Click;
import entity.UrlCountEnd;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import scala.Tuple2;
import utils.CreateClicks;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;

/**
 * ClassName TopNUrl_parll
 *
 * @Auther: 赵繁旗
 * @Date: 2022/4/26 17:24
 * @Description: TopUrl中实现的时候因为使用 WindowAll 导致并行度只有1，效率很低
 * 其他实现方式：
 * 第一步是获取到 url count  所属窗口的结束时间[为了方便调用创建新的类UrlCountEnd]
 * 针对窗口结束时间进行分组调用processFunction处理
 */
public class TopNUrl_parll {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Click> clickDataStreamSource = env.addSource(new CreateClicks());

        SingleOutputStreamOperator<Click> clickWithWM = clickDataStreamSource.assignTimestampsAndWatermarks(WatermarkStrategy
                .<Click>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                .withTimestampAssigner(new SerializableTimestampAssigner<Click>() {
                    @Override
                    public long extractTimestamp(Click element, long recordTimestamp) {

                        return element.timeStamp;
                    }
                }));
        // 统计url count  windowEnd
        // url count aggregateFunction 即可
        // windowEnd 可以使用 windowFunction 获取
        clickWithWM.print("clickSource");
        SingleOutputStreamOperator<UrlCountEnd> urlCountWindow = clickWithWM.keyBy(data -> data.url)
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                // 中间结果使用 URL  urlcountWindow 来保存并将该对象输出
                .aggregate(new AggregateFunction<Click, HashMap<String, Integer>, Integer>() {
                    @Override
                    public HashMap<String, Integer> createAccumulator() {
                        return new HashMap<String, Integer>();
                    }

                    @Override
                    public HashMap<String, Integer> add(Click value, HashMap<String, Integer> accumulator) {

                        if (accumulator.containsKey(value.url)) {
                            accumulator.put(value.url, accumulator.get(value.url) + 1);
                        } else {
                            accumulator.put(value.url, 1);
                        }
                        return accumulator;
                    }

                    @Override
                    public Integer getResult(HashMap<String, Integer> accumulator) {
//                        System.out.println("accumulatord.size()应该为1，验证结果为：" + accumulator.size());// 符合预期，因为前面是按照user.url分组的
                        for (String key : accumulator.keySet()) {
                            return accumulator.get(key);
                        }
                        return null;
                    }

                    @Override
                    public HashMap<String, Integer> merge(HashMap<String, Integer> a, HashMap<String, Integer> b) {
                        return null;
                    }
                }, new WindowFunction<Integer, UrlCountEnd, String, TimeWindow>() {
                    @Override
                    public void apply(String url, TimeWindow window, Iterable<Integer> input, Collector<UrlCountEnd> out) throws Exception {
                        Integer urlCount = input.iterator().next();
                        long endWindow = window.getEnd();
                        out.collect(new UrlCountEnd(url, urlCount, endWindow));
                    }
                });
        urlCountWindow.print("urlcountEnd");// 结果验证正确

        // urlCountWindow 中此时已经保存了所需的数据，此时再次按照window进行分组，从而保证同一个窗口的数据同时执行即可
        SingleOutputStreamOperator<String> result = urlCountWindow.keyBy(data -> data.windowEnd)
                .process(new KeyedProcessFunction<Long, UrlCountEnd, String>() {// 注意参数含义： key input output

                    ListState<UrlCountEnd> listState;

                    // 因为是分布式的，从而要从上下文中去保存一个 ArrayList 保存结果
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        listState = getRuntimeContext().getListState(new ListStateDescriptor<UrlCountEnd>("urlCountEnd", UrlCountEnd.class));
                        super.open(parameters);
                    }

                    @Override
                    public void processElement(UrlCountEnd value, Context ctx, Collector<String> out) throws Exception {
                        listState.add(value);
                        //创建一个触发器，这样等同一个窗口数据到达后触发执行
                        Long currentKey = ctx.getCurrentKey();
                        ctx.timerService().registerEventTimeTimer(currentKey + 1);

                    }

                    // 定时器触发并进行排序
                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        //构建输出
                        StringBuilder sb = new StringBuilder();
                        sb.append("======窗口统计start========\n");

//                        out.collect(sb.toString());
                        Iterable<UrlCountEnd> urlCountEnds = listState.get();
                        ArrayList<UrlCountEnd> list = new ArrayList<>();
                        for (UrlCountEnd urlCountEnd : urlCountEnds) {
                            list.add(urlCountEnd);
                        }
                        list.sort(new Comparator<UrlCountEnd>() {
                            @Override
                            public int compare(UrlCountEnd o1, UrlCountEnd o2) {
                                return o2.count - o1.count;
                            }
                        });
                        for (int i = 0; i < 2; i++) {
                            UrlCountEnd urlCountEnd = list.get(i);
                            sb.append("排名第\t" + (i + 1) + "\t为:\t" + urlCountEnd + "\n");
                        }
                        sb.append("======窗口统计end==========\n");
                        out.collect(sb.toString());
                        super.onTimer(timestamp, ctx, out);
                    }
                });
        result.print("order输出");


        env.execute();


    }
}
