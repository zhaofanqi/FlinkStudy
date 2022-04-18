package transform.basicTransform;

import entity.Click;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * ClassName MyMapper
 *
 * @Auther: 赵繁旗
 * @Date: 2022/4/16 15:32
 * @Description:  map 针对每个单位进行操作
 */
public class MyMap {
    public static void main(String[] args) throws Exception {

        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //数据源接入
        DataStreamSource<Click> sourceFile = env.fromElements(  new Click("zhaofq", "/home", 2000L)
                , new Click("taoxp", "/home", 3000L));


        // 数据转换
        // 方式一： 使用内部类实现接口方法
//        SingleOutputStreamOperator<String> mapResult = sourceFile.map(new MyMapper());
        // 方式二： 直接使用匿名内部类
       /* SingleOutputStreamOperator<String> mapResult = sourceFile.map(new MapFunction<Click, String>() {
            @Override
            public String map(Click value) throws Exception {
                return value.user;
            }
        });*/

        SingleOutputStreamOperator<String> mapResult = sourceFile.map(data -> data.user);
        // 数据输出
        mapResult.print();
        //触发程序调用
        env.execute();

    }

    private static class MyMapper implements MapFunction<Click,String> {
        @Override
        public String map(Click value) throws Exception {
            return value.user;
        }
    }



}
