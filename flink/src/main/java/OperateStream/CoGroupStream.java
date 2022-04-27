package OperateStream;

import entity.Click;
import entity.UrlCountEnd;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import utils.CreateClicks;

import java.time.Duration;

/**
 * ClassName CoGroupStream
 *
 * @Auther: 赵繁旗
 * @Date: 2022/4/27 21:37
 * @Description:    CoGroupStream 相较于 join 更底层.并且输出的是全关联结果
 *                                  语法： stream.coGroup(stream)
 *                                              .where()
 *                                              .equalTo()
 *                                              .window()
 *                                              .apply(CoGroupFunction)
 */
public class CoGroupStream {
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

        // 会输出每个窗口的数据，即使没关联的也会输出
        DataStream<String> result = clickwithWM.coGroup(urlCountEndWM)
                .where(data -> data.url)
                .equalTo(data -> data.url)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .apply(new CoGroupFunction<Click, UrlCountEnd, String>() {
                    @Override
                    public void coGroup(Iterable<Click> first, Iterable<UrlCountEnd> second, Collector<String> out) throws Exception {
                        out.collect(first+"=>"+second);
                    }
                });

        result.print("result");

        env.execute();

    }
}
