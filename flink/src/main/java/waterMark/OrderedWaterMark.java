package waterMark;

import entity.Click;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * ClassName OrderedWaterMark
 *
 * @Auther: 赵繁旗
 * @Date: 2022/4/21 14:20
 * @Description:
 */
public class OrderedWaterMark {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setAutoWatermarkInterval(100);//默认水位线是200mills一次
        SingleOutputStreamOperator<Click> clicks = env.readTextFile("./flink/src/main/resources/clicks.txt")
                .map(x -> {
                    String[] fields = x.split(",");
                    return new Click(fields[0], fields[1], Long.parseLong(fields[2]));
                });

        SingleOutputStreamOperator<Click> orderedWaterMark = clicks.assignTimestampsAndWatermarks(WatermarkStrategy
                .<Click>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<Click>() {
                    @Override
                    public long extractTimestamp(Click element, long recordTimestamp) {
                        return element.timeStamp;
                    }
                })
        );
        orderedWaterMark.print();
        env.execute();

    }
}
