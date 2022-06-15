package zy_juc.v20220531.capter3CompleteFuture;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

/**
 * ClassName FutureTask01
 *
 * @Auther: 赵繁旗
 * @Date: 2022/6/10 14:04
 * @Description:    FutureTask的特点： 异步+多线程+有返回值
 *                     Callable  ---->返回值
 *                     Future: 异步
 *                     Runnable:多线程
 *                    FutureTask 实现RunnableFuture借口，构造器注入 Callable
 */
public class FutureTask01 {
    public static void main(String[] args) throws ExecutionException, InterruptedException {

        FutureTask<String> futureTask = new FutureTask<>(() -> {
            System.out.println("coming in futureTask");
            return "hello,call";
        });
        Thread t1 = new Thread(futureTask, "t1");
        t1.start();
        System.out.println(futureTask.get());

    }
}
