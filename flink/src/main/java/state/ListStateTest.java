package state;

import entity.Click;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
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

import java.time.Duration;
import java.util.*;

/**
 * ClassName ListStateTest
 *
 * @Auther: 赵繁旗
 * @Date: 2022/4/29 09:17
 * @Description: ListState 去实现历史数据的 topN
 * 基于用户的历史访问次数排序
 */
public class ListStateTest {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Click> clickSource = env.addSource(new CreateClicks());

        SingleOutputStreamOperator<Click> clickWM = clickSource.assignTimestampsAndWatermarks(WatermarkStrategy
                .<Click>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<Click>() {
                    @Override
                    public long extractTimestamp(Click element, long recordTimestamp) {
                        return element.timeStamp;
                    }
                }));

        clickWM.print("click");
        SingleOutputStreamOperator<List<Tuple2<String, Integer>>> list_state = clickWM.keyBy(data -> data.user)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new ProcessWindowFunction<Click, List<Tuple2<String, Integer>>, String, TimeWindow>() {

                    // map(key,value) key:user value 为count
                    ListState<Map<String, Integer>> listState;

                    @Override
                    public void open(Configuration parameters) throws Exception {

                        listState = getRuntimeContext().getListState(new ListStateDescriptor<Map<String, Integer>>("list state", Types.MAP(Types.STRING, Types.INT)));
                    }

                    @Override
                    public void process(String s, Context context, Iterable<Click> elements, Collector<List<Tuple2<String, Integer>>> out) throws Exception {
                        Map<String, Integer> map = listState.get().iterator().hasNext()
                                ? listState.get().iterator().next()
                                : new HashMap<String, Integer>();

                        for (Click element : elements) {
                            if (map.containsKey(element.user)) {
                                map.put(element.user, map.get(element.user) + 1);
                            } else {
                                map.put(element.user, 1);
                            }
                        }
                        listState.add(map);

                        // map排序输出
                        Map<String, Integer> resultMap = listState.get().iterator().next();
                        ArrayList<Tuple2<String, Integer>> list = new ArrayList<>();
                        for (String key : resultMap.keySet()) {
                            list.add(Tuple2.of(key, resultMap.get(key)));
                        }
                        list.sort(new Comparator<Tuple2<String, Integer>>() {
                            @Override
                            public int compare(Tuple2<String, Integer> o1, Tuple2<String, Integer> o2) {
                                return o2.f1 - o1.f1;
                            }
                        });


                        //遍历list
                        StringBuilder sb = new StringBuilder();
                        for (int i = 0; i < list.size(); i++) {
                            sb.append(list.get(i).f0 + "\t" + list.get(i).f1 + "\t");
                        }
                        sb.append("===============\n");
                        out.collect(list);
                    }
                });
        list_state.print("final").setParallelism(1);
//        topN.print("final");

        env.execute();


    }
}
