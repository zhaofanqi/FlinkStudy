package waterMark;

import entity.Click;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

/**
 * ClassName OrderedWaterMark
 *
 * @Auther: 赵繁旗
 * @Date: 2022/4/21 13:56
 * @Description: 水位线：提取数据中的时间戳作为标记，当数据没有流转到下游算子时，
 * watermark 可以被下游算子感知，从而避免窗口数据没有到达时，下游算子也处于等待状态
 * 生成方式：事件触发
 * 周期性
 * 因为数据传输存在延迟，为了窗口可以将指定事件范围内数据包括，可在水位线基础上+一定的延迟时间。此处涉及准确性与及时性的平衡
 * 水位线越贴近源头设置越好（数据尽可能早的有时间）
 */
public class OutOfOrderWaterMark {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setAutoWatermarkInterval(100);//默认水位线是200mills一次
        SingleOutputStreamOperator<Click> clicks = env.readTextFile("./flink/src/main/resources/clicks.txt")
                .map(x -> {
                    String[] fields = x.split(",");
                    return new Click(fields[0], fields[1], Long.parseLong(fields[2]));
                });


        SingleOutputStreamOperator<Click> outOfOrderWatermark =
                clicks.assignTimestampsAndWatermarks(//插入水位线
                        WatermarkStrategy
                                .<Click>forBoundedOutOfOrderness(Duration.ofSeconds(2))// 针对乱序插入水位线，延迟设置为2秒
                                .withTimestampAssigner(new SerializableTimestampAssigner<Click>() {// 抽取时间戳
                                    @Override
                                    public long extractTimestamp(Click element, long recordTimestamp) {
                                        return element.timeStamp;
                                    }
                                }));

        outOfOrderWatermark.print();
        env.execute();


    }
}
