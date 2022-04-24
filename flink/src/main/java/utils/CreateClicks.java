package utils;

import entity.Click;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * ClassName CreateClicks
 *
 * @Auther: 赵繁旗
 * @Date: 2022/4/24 10:21
 * @Description:
 */
public class CreateClicks implements SourceFunction<Click> {
    Boolean Running = true;
    Random random = new Random();
    // 每秒都生成一条数据
    @Override
    public void run(SourceContext<Click> ctx) throws Exception {
        String[] user = {"zhaofq", "taoxp", "zhangxy"};
        String[] urls = {"/home", "/product", "/carts", "/food"};
        while (Running) {
            ctx.collect(new Click(user[random.nextInt(user.length)], urls[random.nextInt(urls.length)], System.currentTimeMillis()));
            Thread.sleep(TimeUnit.SECONDS.toMillis(1));
        }
    }
    @Override
    public void cancel() {
        Running = false;
    }
}
