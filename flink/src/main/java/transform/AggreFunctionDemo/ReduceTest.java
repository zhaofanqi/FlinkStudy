package transform.AggreFunctionDemo;

import entity.Click;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.LongValue;
import org.apache.flink.util.Collector;

/**
 * ClassName ReduceTest
 *
 * @Auther: 赵繁旗
 * @Date: 2022/4/18 14:26
 * @Description: 获取click事件中点击次数最多的用户以及次数
 *          通过案例学习到： 聚合之前必须按键分组
 */
public class ReduceTest {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 数据转换
        // 添加输入源  此处先不做处理，也就是没有着急返回元组
        DataStreamSource<String> fileStream = env.readTextFile("./flink/src/main/resources/clicks.txt");
        SingleOutputStreamOperator<Click> clickSource = fileStream.flatMap(new FlatMapFunction<String, Click>() {
            @Override
            public void flatMap(String value, Collector<Click> out) throws Exception {
                String[] fields = value.split(",");
                out.collect(new Click(fields[0], fields[1], Long.valueOf(fields[2])));
            }
        });
        // 构建<user,1>
        SingleOutputStreamOperator<Tuple2<String, Integer>> userClick = clickSource.flatMap(new FlatMapFunction<Click, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(Click value, Collector<Tuple2<String, Integer>> out) throws Exception {
                out.collect(new Tuple2<>(value.user, 1));
            }
        });
        // 聚合之前必须分组 ,并将每个用户的结果统计
        SingleOutputStreamOperator<Tuple2<String, Integer>> userCount = userClick.keyBy(data -> data.f0).reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                return Tuple2.of(value1.f0, value1.f1 + value2.f1);
            }
        });
        // 取出最活跃的用户
        SingleOutputStreamOperator<Tuple2<String, Integer>> reduceResult = userCount.keyBy(data -> "key").reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                return value1.f1 > value2.f1 ? value1 : value2;
            }
        });
        // 数据输出
        reduceResult.print();
        // 触发程序
        env.execute();
    }
}
