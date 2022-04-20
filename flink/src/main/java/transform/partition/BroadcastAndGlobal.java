package transform.partition;

import entity.Click;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * ClassName broadcastAndGlobal
 *
 * @Auther: 赵繁旗
 * @Date: 2022/4/20 09:53
 * @Description: 分区有： shuffle/rebalance（default）/broadcast/global/partitionCustom
 * 演示 broadCast global 的使用
 * broadcast   是将数据发送到所有分区
 * global      是将数据发送到一个分区
 */
public class BroadcastAndGlobal {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> fileStream = env.readTextFile("./flink/src/main/resources/clicks.txt");

        SingleOutputStreamOperator<Click> clicks = fileStream.map(new MapFunction<String, Click>() {
            @Override
            public Click map(String value) throws Exception {
                String[] fields = value.split(",");
                return new Click(fields[0], fields[1], Long.parseLong(fields[2]));
            }
        });
        // 方式一 ：正常输出
//        clicks.print("default").setParallelism(2);
        // 方式二 ： broadCast
//        clicks.broadcast().print("broadcast").setParallelism(2);
        // 方式三 ： global
        clicks.global().print("global").setParallelism(2);
        env.execute();


    }
}
