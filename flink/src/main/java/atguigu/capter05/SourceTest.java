package atguigu.capter05;

import entity.Click;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

/**
 * ClassName SourceTest
 *
 * @Auther: 赵繁旗
 * @Date: 2022/4/13 14:56
 * @Description:  从多种数据源获取数据
 *              此处需要注意：pojo是可以序列化的
 */
public class SourceTest {
    public static void main(String[] args) throws Exception {
//        获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
//        获取数据源
//        1 从文本文件
        DataStreamSource<String> stream1 = env.readTextFile("./flink/src/main/resources/clicks.txt");
//        2 从集合中
        ArrayList<String> list1 = new ArrayList<>();
        list1.add("hello");
        list1.add("tao");
        DataStreamSource<String> stream2 = (DataStreamSource<String>) env.fromCollection(list1);
//        3 从集合类
        ArrayList<Click> clicks = new ArrayList<>();
        clicks.add(new Click("zhaofq","/home",2000L));
        clicks.add(new Click("taoxp","/home",3000L));
        DataStreamSource<Click> stream3 = env.fromCollection(clicks);
//        4 从元素中
        DataStreamSource<Click> stream4 = env.fromElements(
                new Click("zhaofq", "/home", 2000L), new Click("taoxp", "/home", 3000L)
        );
//        进行转换操作
//        输出
        stream1.print("1");
        stream2.print("2");
        stream3.print("3");
        stream4.print("4");

//        启动程序
        env.execute();
    }
}
