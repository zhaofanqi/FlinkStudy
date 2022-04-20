package transform.partition;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * ClassName ShuffleAndRebalance
 *
 * @Auther: 赵繁旗
 * @Date: 2022/4/20 10:08
 * @Description:    分区 ： rebalance/shuffle/global/broadcat/partitionCustom
 *                          rebalance : 默认分区，round-robin 方式
 *                          shuffle ：随机发送

 */
public class ShuffleAndRebalance {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Integer> numSource = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8);
        // 随机发送，每个节点获取的数据量可能不同
        numSource.shuffle().print().setParallelism(2);
//        轮询方式
//        numSource.rebalance().print().setParallelism(2);

        env.execute();

    }
}
