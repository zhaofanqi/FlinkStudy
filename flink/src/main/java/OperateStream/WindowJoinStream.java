package OperateStream;

import entity.Click;
import entity.UrlCountEnd;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import utils.CreateClicks;

import java.time.Duration;

/**
 * ClassName WindowJoinStream
 *
 * @Auther: 赵繁旗
 * @Date: 2022/4/27 19:03
 * @Description:   窗口连接的语法： stream1.join(stream2)
 *                                       .where(KeySelector)
 *                                       .equalTo(KeySelector)
 *                                       .window(Sliding/Tumbling/Session)
 *                                       .apply(JoinFunction)
 *
 *                               ==》inner join
 */
public class WindowJoinStream {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        DataStreamSource<Click> clickDataStreamSource = env.addSource(new CreateClicks());
        DataStreamSource<UrlCountEnd> urlCountEndSource = env.fromElements(
                new UrlCountEnd("/product", 10, System.currentTimeMillis()+1000),
                new UrlCountEnd("/product", 10, System.currentTimeMillis() + 2000),
                new UrlCountEnd("/food", 25, System.currentTimeMillis() + 7000),
                new UrlCountEnd("/food", 30, System.currentTimeMillis() + 8000)
        );
        SingleOutputStreamOperator<Click> clickwithWM = clickDataStreamSource.assignTimestampsAndWatermarks(WatermarkStrategy
                .<Click>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<Click>() {
                    @Override
                    public long extractTimestamp(Click element, long recordTimestamp) {
                        return element.timeStamp;
                    }
                }));

        clickwithWM.print("click");
        SingleOutputStreamOperator<UrlCountEnd> urlCountEndWM = urlCountEndSource.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<UrlCountEnd>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<UrlCountEnd>() {
                            @Override
                            public long extractTimestamp(UrlCountEnd element, long recordTimestamp) {
                                return element.windowEnd;
                            }
                        })
        );
        urlCountEndWM.print("url");

        // 窗口连接

        DataStream<String> result = clickwithWM.join(urlCountEndWM)
                .where(data -> data.url)
                .equalTo(data -> data.url)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .apply(new JoinFunction<Click, UrlCountEnd, String>() {
                    @Override
                    public String join(Click first, UrlCountEnd second) throws Exception {
                        StringBuilder sb = new StringBuilder();
                        sb.append(first.toString() + "\n" + second.toString());
                        return sb.toString();
                    }
                });

        result.print("result");

        env.execute();

    }
}
