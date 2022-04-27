package OperateStream;

import entity.Click;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import utils.CreateClicks;

import java.time.Duration;

/**
 * ClassName OutputStream
 *
 * @Auther: 赵繁旗
 * @Date: 2022/4/27 08:11
 * @Description:    基于流的操作：
 *                          分流；
 *                          流的合并：流中数据类型一致
 *                          流的连接：流中类型不一致，可经过 map  flatMap process转化后返回相同类型
 */
public class OutputToSplitStream {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Click> clickSource = env.addSource(new CreateClicks());

        SingleOutputStreamOperator<Click> clickwithWM = clickSource.assignTimestampsAndWatermarks(WatermarkStrategy
                .<Click>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                .withTimestampAssigner(new SerializableTimestampAssigner<Click>() {
                    @Override
                    public long extractTimestamp(Click element, long recordTimestamp) {
                        return element.timeStamp;
                    }
                }));
        OutputTag<Click> zhaofqSide = new OutputTag<Click>("zhaofq") {};
        OutputTag<Click> taoxpSide = new OutputTag<Click>("taoxp") {};
        OutputTag<Click> zhangxySide = new OutputTag<Click>("zhangxy") {};

        SingleOutputStreamOperator<Click> outputStream = clickwithWM.process(new ProcessFunction<Click, Click>() {
            @Override
            public void processElement(Click value, Context ctx, Collector<Click> out) throws Exception {
                if (value.user.equals("zhaofq")) {
                    ctx.output(zhaofqSide, value);
                } else if (value.user.equals("taoxp")) {
                    ctx.output(taoxpSide, value);
                } else {
                    ctx.output(zhangxySide, value);
                }
            }
        });

        outputStream.print("应该是没数据的");//因为没有在processFunction 中收集数据
        DataStream<Click> zhaofqSideOutput = outputStream.getSideOutput(zhaofqSide);
        DataStream<Click> taoxpSideOutput = outputStream.getSideOutput(zhaofqSide);
        DataStream<Click> zhangxySideOutput = outputStream.getSideOutput(zhaofqSide);

        zhaofqSideOutput.print("zhaofqSideOutput");
        taoxpSideOutput.print("taoxpSideOutput");
        zhangxySideOutput.print("zhangxySideOutput");

        env.execute();
    }
}
