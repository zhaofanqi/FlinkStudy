package transform;

import entity.Click;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * ClassName MyRichFunctionTest
 *
 * @Auther: 赵繁旗
 * @Date: 2022/4/18 17:11
 * @Description:  使用 RichMapFunction
 *                      此时 map(只能是继承抽象类实现抽象方法，或者匿名内部类，不能是lambda)
 */
public class MyRichFunctionTest {
    public static void main(String[] args) throws Exception {

        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 获取输入源
        DataStreamSource<Click> eleSource = env.fromElements(
                new Click("zhaofq", "/", 2000L)
                , new Click("zhangsan", "/h", 2000L)
                , new Click("taoxp", "/home", 3000L)
                , new Click("zhaofq", "/production", 3000L)
                , new Click("zhangsan", "/car", 4000L)
                , new Click("taoxp", "/address", 5000L)
        );
        // 转换
        SingleOutputStreamOperator<Integer> mapStream = eleSource.map(new RichMapFunction<Click, Integer>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                System.out.println("this is  "+getRuntimeContext().getIndexOfThisSubtask()+" 号task 启动 ");
            }

            @Override
            public void close() throws Exception {
                super.close();
                System.out.println("this is  "+getRuntimeContext().getIndexOfThisSubtask()+" 号task 关闭 ");
            }

            @Override
            public Integer map(Click value) throws Exception {
                return value.url.length();
            }
        }).setParallelism(2);
        // 输出
        mapStream.print();
        // 触发执行
        env.execute();
    }
}
