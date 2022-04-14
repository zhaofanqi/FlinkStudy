package input;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


/**
 * ClassName SocketInput
 *
 * @Auther: 赵繁旗
 * @Date: 2021/11/10 14:23
 * @Description:
 */
public class SocketInput {

    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //加载/初始化数据源
        DataStreamSource<String> dataSource = env.socketTextStream("127.0.0.1", 9999);
        // 进行转换操作
        SingleOutputStreamOperator<Tuple2<String,Integer>> wordAndOnes = dataSource.flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String,Integer>> collector) throws Exception {
                String[] words = s.split(" ");
                for(String word:words ){
                    // 将结果存入收集器
                   collector.collect(Tuple2.of(word,1));
                }

            }
        });
        SingleOutputStreamOperator<Tuple2<String, Integer>> sumWord = wordAndOnes.keyBy(0).sum(1);
        // 指定结果输出位置
//        wordAndOnes.print();
        sumWord.print();
        // 程序执行
        env.execute();




    }
}
