package OperateStream;

import entity.Click;
import entity.Order;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import utils.CreateClicks;

import javax.xml.crypto.KeySelector;
import java.time.Duration;

/**
 * ClassName IntervalJoinStream
 *
 * @Auther: 赵繁旗
 * @Date: 2022/4/27 20:17
 * @Description:        窗口连接
 *                      间隔连接    stream1.keyby()
 *                                        .intervaljoin(keyedStream)
 *                                        .between(lowbound,upperbound)
 *                                        .
 *                                        可以以主流的数据单独开窗（往前往后）
 *                      案例场景：一个下订单用户，查看她下订单前10秒和后十秒的浏览记录
 *                              主流是订单数据 ，被连接流为浏览记录,依据 user 做关联
 *
 */
public class IntervalJoinStream {
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
        DataStreamSource<Order> orderSource = env.fromElements(
                new Order("zhaofq", "home", System.currentTimeMillis()),
                new Order("taoxp", "home", System.currentTimeMillis() + 10000),
                new Order("zhaofq", "home", System.currentTimeMillis() + 20000)
        );
        SingleOutputStreamOperator<Order> orderWM = orderSource.assignTimestampsAndWatermarks(WatermarkStrategy
                .<Order>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<Order>() {
                    @Override
                    public long extractTimestamp(Order element, long recordTimestamp) {
                        return element.timestamp;
                    }
                }));
        clickWM.print("点击");
        orderWM.print("订单");
        SingleOutputStreamOperator<String> joinResult = orderWM.keyBy(data -> data.user)
                .intervalJoin(clickWM.keyBy(data -> data.user))
                .between(Time.seconds(-5), Time.seconds(10))
                .process(new ProcessJoinFunction<Order, Click, String>() {
                    @Override
                    public void processElement(Order left, Click right, Context ctx, Collector<String> out) throws Exception {
                        out.collect(right + "可能引起=>" + left);
                    }
                });
        joinResult.print("结果");


        env.execute();
    }
}
