package transform.basicTransform;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * ClassName MyFilter
 *
 * @Auther: 赵繁旗
 * @Date: 2022/4/16 16:17
 * @Description:
 */
public class MyFilter {
    public static void main(String[] args) throws Exception {

        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 获取输入源
        DataStreamSource<String> filesource = env.readTextFile("./flink/src/main/resources/clicks.txt");
        // transform
        SingleOutputStreamOperator<String> filterResult = filesource.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {

                return value.contains("zhangsan");
            }
        });
        // 输出
        filterResult.print();
        // 触发程序
        env.execute();

    }
}
