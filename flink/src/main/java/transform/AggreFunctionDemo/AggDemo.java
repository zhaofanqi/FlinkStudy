package transform.AggreFunctionDemo;

import entity.Click;
import entity.Users;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * ClassName AggDemo
 *
 * @Auther: 赵繁旗
 * @Date: 2022/4/18 10:49
 * @Description:  聚合算子demo；
 *              注意：max返回的是首条记录+指定max字段最大值
 *                   maxBy返回的是指定max字段最大值的完整记录
 *                   实际应用场景： 分区字段a 排序字段b  然后是前几的
 */
public class AggDemo {
    public static void main(String[] args) throws Exception {

        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 加入数据源
        DataStreamSource<Click> clickDataStreamSource = env.fromElements(
                new Click("zhaofq", "/home", 2000L)
                , new Click("zhangsan", "/home", 2000L)
                , new Click("taoxp", "/home", 3000L)
                , new Click("zhaofq", "/prod", 3000L)
                , new Click("zhangsan", "/prod", 4000L)
                , new Click("taoxp", "/prod", 5000L)
        );
        // 转换操作
        // KeySelector 输入记录，返回分区字段
        KeyedStream<Click, String> keyedStream = clickDataStreamSource.keyBy(new KeySelector<Click, String>() {
            @Override
            public String getKey(Click value) throws Exception {
                return value.user;
            }
        });
        // 输出
//        keyedStream.max("timeStamp").print("max: ");
//        keyedStream.maxBy("timeStamp").print("maxBy: ");

        // test 2 取出各个性别下，ID最大的记录
        // Noted 此种方式得到的会输出多条记录，并非仅一条.因为代码是按照流处理方式，来一条处理一条，如果想仅仅输出一条，则设置运行模式为batch即可
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        DataStreamSource<Users> userSource = env.fromElements(
                new Users(1, "zhaofq", "male")
                , new Users(5, "wangwu", "male")
                , new Users(2, "taoxp", "female")
                , new Users(4, "lisi", "male")
                , new Users(3, "zhangsan", "male")

        );
        // 假设数据量巨大，此时需要先按逻辑分区处理
        KeyedStream<Users, String> keyedStream2 = userSource.keyBy(new KeySelector<Users, String>() {
            @Override
            public String getKey(Users value) throws Exception {
                return value.getSex();
            }
        });

        keyedStream2.maxBy("id").print();
        // 分组最大值

        // 触发程序
        env.execute();

    }
}
