package transform.partition;

import entity.Click;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * ClassName MyPartitionCustom
 *
 * @Auther: 赵繁旗
 * @Date: 2022/4/20 10:15
 * @Description:    分区： rebalance/shuffle/global/broadcast/partitionCustom
 *                  partitionCustom 自定义分区方式
 */
public class MyPartitionCustom {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> fileSource = env.readTextFile("./flink/src/main/resources/clicks.txt");

        SingleOutputStreamOperator<Click> clicks = fileSource.map(new MapFunction<String, Click>() {
            @Override
            public Click map(String value) throws Exception {
                String[] fields = value.split(",");
                return new Click(fields[0], fields[1], Long.parseLong(fields[2]));
            }
        });

//        clicks.print().setParallelism(2);
        DataStream<Click> partitionedClicks = clicks.partitionCustom(new Partitioner<Integer>() {
            @Override
            public int partition(Integer key, int numPartitions) {
                return key;
            }
        }, new KeySelector<Click, Integer>() {
            @Override
            public Integer getKey(Click value) throws Exception {

                return value.user.equals("zhangsan")?0:1;

            }
        });

        partitionedClicks.print().setParallelism(2);
        env.execute();


    }
}
