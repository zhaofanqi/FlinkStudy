package source;

import entity.Click;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * ClassName SelfSource
 *
 * @Auther: 赵繁旗
 * @Date: 2022/4/16 15:33
 * @Description: 自定义数据源 实际使用时 并行度注意
 */
public class SelfSource implements SourceFunction<Click> {

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
}
