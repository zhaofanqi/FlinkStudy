package utils;

import entity.Click;
import entity.Users;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * ClassName CreateUsers
 *
 * @Auther: 赵繁旗
 * @Date: 2022/5/5 10:30
 * @Description:
 */
public class CreateUsers implements SourceFunction<Users>{
    Boolean Running = true;
    Random random = new Random();
    // 每秒都生成一条数据
    @Override
    public void run(SourceFunction.SourceContext<Users> ctx) throws Exception {
        String[] username = {"zhaofq", "taoxp", "zhangxy"};
        int i=0;
        String[] sex = {"male", "female"};
        while (Running) {
            ctx.collect(new Users(i++,username[random.nextInt(username.length)],sex[random.nextInt(sex.length)]));
            Thread.sleep(TimeUnit.SECONDS.toMillis(1));
        }
    }
    @Override
    public void cancel() {
        Running = false;
    }
}
