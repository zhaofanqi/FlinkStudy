package source;

import entity.Click;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.sql.Timestamp;
import java.util.Random;

/**
 * ClassName FlinkAddSource
 *
 * @Auther: 赵繁旗
 * @Date: 2022/4/16 15:40
 * @Description:
 */
public class FlinkAddSource {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //方式一 ：匿名内部类
       /* DataStreamSource<Click> clickDataStreamSource = env.addSource(new SourceFunction<Click>() {
             // 获取数据源,添加数据源时，必然有输入有停止
            volatile Boolean running = true;
            String users[] = {"zhaofq", "tao", "haha"};
            String urls[] = {"/home", "/cart", "/prod"};
            Random random = new Random();

            @Override
            public void run(SourceContext<Click> sourceContext) throws Exception {
                while (running) {
                    Click click = new Click(users[random.nextInt(users.length)], urls[random.nextInt(urls.length)], System.currentTimeMillis());
                    sourceContext.collect(click);
                    Thread.sleep(1000L);
                }
            }

            @Override
            public void cancel() {
                running = false;
            }
        });*/
        // 方式二：使用类实现接口
        DataStreamSource<Click> clickDataStreamSource = env.addSource(new SelfSource());

        // 进行转换
        clickDataStreamSource.print();
        // 输出
        // 触发程序
        env.execute();
    }
}
