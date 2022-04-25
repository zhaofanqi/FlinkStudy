package processFunction;

import com.sun.tools.corba.se.idl.constExpr.Times;
import entity.Click;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import utils.CreateClicks;

import java.sql.Timestamp;
import java.time.Duration;

/**
 * ClassName ProcessFunctionTest
 *
 * @Auther: 赵繁旗
 * @Date: 2022/4/25 09:59
 * @Description: 处理函数：ProcessFunction         不可使用TimeService的定时任务
 * KedProcessFunction      可使用TimerService
 * ProcessWindowFunction   窗口处理
 */
public class ProcessFunctionTest {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Click> clickDataStreamSource = env.addSource(new CreateClicks());

        SingleOutputStreamOperator<Click> clickWithWaterMark = clickDataStreamSource.assignTimestampsAndWatermarks(
                WatermarkStrategy.<Click>forBoundedOutOfOrderness(Duration.ofSeconds(10))//水位生成器
                        .withTimestampAssigner(new SerializableTimestampAssigner<Click>() {// 水位线字段
                            @Override
                            public long extractTimestamp(Click element, long recordTimestamp) {
                                recordTimestamp = element.timeStamp;
                                return element.timeStamp;
                            }

                        })

        );
        Thread.sleep(1000L);
        OutputTag<Click> clickOutputTag = new OutputTag<Click>("late") {
        };
        SingleOutputStreamOperator<String> processResult = clickWithWaterMark
                .process(new ProcessFunction<Click, String>() {
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        Thread.sleep(1000);
                        super.open(parameters);
                    }

                    @Override
                    public void processElement(Click value, Context ctx, Collector<String> out) throws Exception {
                        // Noted: 首条数据进入时水位线为负数 ,open方法调用时，此处仍然为负数，待理解原因
                        // 查看源码  The method is passed the previously assigned timestamp of the element. That previous
                        //     * timestamp may have been assigned from a previous assigner. If the element did not carry a
                        //     * timestamp before, this value is {@link #NO_TIMESTAMP} (= {@code Long.MIN_VALUE}: {@value
                        //     * Long#MIN_VALUE}) 此处说明：当前element获取到的水位线是前一个元素的水位线，因为首个元素的水位线为负数
                        Timestamp currentPT = new Timestamp(value.timeStamp);
                        Timestamp currentWM = new Timestamp(ctx.timerService().currentWatermark());
                        // 注意数据进入以后，此处的输出，水位线可能还没生成呢
                        out.collect(value.user + "数据时间：" + currentPT + "\t 水位线：" + currentWM);
                        if (value.user.equals("zhaofq")) {
                            ctx.output(clickOutputTag, value);
                        }

                    }
                });
        processResult.print("processfunction : ");
        DataStream<Click> sideOutput = processResult.getSideOutput(clickOutputTag);
        // 从这里的输出可以看出：数据时间戳+上一条数据时间戳-1（作为当前数据的水位线）
        sideOutput.process(new ProcessFunction<Click, String>() {
            @Override
            public void processElement(Click value, Context ctx, Collector<String> out) throws Exception {
                Timestamp currentPT = new Timestamp(value.timeStamp);
                Timestamp currentWM = new Timestamp(ctx.timerService().currentWatermark());
                // 注意数据进入以后，此处的输出，水位线可能还没生成呢
                out.collect(value.user + "数据时间：" + currentPT + "\t 水位线：" + currentWM);
                if (value.user.equals("zhaofq")) {
                    ctx.output(clickOutputTag, value);
                }
            }
        }).print("侧输出流收集的数据");
        env.execute();

    }
}
