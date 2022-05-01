package state;

import entity.Click;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import utils.CreateClicks;

import java.sql.Timestamp;

/**
 * ClassName MapStateTest
 *
 * @Auther: 赵繁旗
 * @Date: 2022/4/30 19:31
 * @Description:        使用MapState模拟 Window 统计url count
 *                      由于需要使用到定时器，上下文状态 从而要有processFunction
 *                      因为使用processFunction 因为使用keyBy
 *                      按照url 分组，统计窗口中的次数
 */
public class MapStateTest {
    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Click> clickSource = env.addSource(new CreateClicks());

        SingleOutputStreamOperator<Click> clickWM = clickSource.assignTimestampsAndWatermarks(WatermarkStrategy
                .<Click>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<Click>() {
                    @Override
                    public long extractTimestamp(Click element, long recordTimestamp) {
                        return element.timeStamp;
                    }
                })
        );

        clickWM.print();
        SingleOutputStreamOperator<String> mapResult = clickWM.keyBy(data -> data.url)
                .process(new KeyedProcessFunction<String, Click, String>() {
                     Long windowSize;
                    MapState<Long,Long> mapState;



                    @Override
                    public void open(Configuration parameters) throws Exception {
                        windowSize=10000L;
                        mapState=getRuntimeContext().getMapState(new MapStateDescriptor<Long, Long>("map state",Long.TYPE,Long.TYPE));
                    }



                    @Override
                    public void processElement(Click value, Context ctx, Collector<String> out) throws Exception {
                        //确定数据属于那个窗口：依据窗口开始时间计算
                        long windowStart = value.timeStamp / windowSize * windowSize;
                        long windowEnd=windowStart+windowSize-1;
                        if (!mapState.contains(windowStart)){
                            mapState.put(windowStart,1L);
                        }else {
                            mapState.put(windowStart,mapState.get(windowStart)+1);
                        }
                        // 底层会自动将重复的定时器去重
                        ctx.timerService().registerEventTimeTimer(windowEnd);



                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        long windowEnd =timestamp+1;
                        long windowStart=windowEnd-windowSize;

                        out.collect(
                                "窗口"+new Timestamp(windowStart)+"\t~\t"
                                +new Timestamp(windowEnd)
                                +"url\t"+ctx.getCurrentKey()
                                +"\tcount\t"+mapState.get(windowStart)

                        );

                        //定时器要及时销毁
                        mapState.remove(timestamp);
                    }

                });


        mapResult.print("result");
        env.execute();
    }
}

