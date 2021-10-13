package demo;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author: zhaofanqi
 * @TIME: Created in 14:25 2020/12/18
 * @Desc:
 */

public class StreamingWorldCount {
    public static void main(String[] args) throws Exception {
        // 获取上下文 环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        System.out.println(env.getConfig());
        env.setParallelism(10);// 即使超过实际的核数，也会按照最大的核数进行计算
        // 定义文件源
        String filePath = "flink/src/main/resources/hello.txt";
        // 针对获取的数据进行分词  汇总

        DataStream<Tuple2<String, Integer>> resultStream = env.readTextFile(filePath).flatMap(new MyFlatMapFunction()).keyBy(0).sum(1);
        //  输出
        resultStream.print();
        // 启动任务  ,前面只是定义任务
        env.execute();
    }
}
