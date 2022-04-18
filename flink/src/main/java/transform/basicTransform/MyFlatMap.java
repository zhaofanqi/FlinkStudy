package transform.basicTransform;

import entity.Click;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * ClassName MyFlatMap
 *
 * @Auther: 赵繁旗
 * @Date: 2022/4/18 09:25
 * @Description:  Noted: Map 和 FlatMap 都可能是类型擦出（输入与输出类型不一致）
 *              FlatMap 是将数据打散的收集输出的。所以，收集的过程可以实现数据的过滤
 */
public class MyFlatMap {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 获取数据源
        DataStreamSource<Click> clickDataStreamSource = env.fromElements(new Click("zhaofq", "/home", 2000L)
                , new Click("zhangsan", "/home", 2000L)
                ,new Click("taoxp", "/home", 3000L));
        // 转换
        // 输出
        /*clickDataStreamSource.flatMap(new FlatMapFunction<Click, String>() {
            @Override
            public void flatMap(Click value, Collector<String> out) throws Exception {
                //此处可实现数据过滤
                if (value.user.equals("zhaofq")){
                    out.collect(value.toString());
                }else if (value.user.equals("taoxp")){
                    out.collect(value.user);
                }
            }
        }).print();*/

        SingleOutputStreamOperator<String> flatMapResult = clickDataStreamSource.flatMap(new MyselfFlatMap());

        flatMapResult.print();
        // 触发执行
        env.execute();


    }

    public  static class  MyselfFlatMap implements FlatMapFunction<Click,String>{

        @Override
        public void flatMap(Click value, Collector<String> out) throws Exception {
            if (value.user.equals("zhaofq")){
                out.collect(value.toString());
            }else if (value.user.equals("taoxp")){
                out.collect(value.user);
            }
        }
    }

}
