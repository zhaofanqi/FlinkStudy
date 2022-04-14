package demo;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * ClassName BatchWordCount
 *
 * @Auther: 赵繁旗
 * @Date: 2022/4/1 15:14
 * @Description:
 */
public class BatchWordCount {
    public static void main(String[] args) throws Exception {
        // 1 获取批处理执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // 2 获取输入源头
        DataSource<String> line = env.readTextFile("flink/src/main/resources/hello.txt");
        // 3 逻辑处理
        FlatMapOperator<String, Tuple2<String, Long>> wordAndOne = line.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Long>> collector) throws Exception {
                String[] words = s.split(" ");
                for (String word : words) {
                    collector.collect(Tuple2.of(word,1L));
                }
            }
        });

        AggregateOperator<Tuple2<String, Long>> wordCounts = wordAndOne.groupBy(0).sum(1);
        // 4 输出
        wordCounts.print();
    }
}
