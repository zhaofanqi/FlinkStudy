package zy_juc.v20220531.capter3CompleteFuture;

import java.util.concurrent.*;

/**
 * ClassName CompletableFuture
 *
 * @Auther: 赵繁旗
 * @Date: 2022/6/13 17:15
 * @Description:  获取CompletableFuture的处理结果
 *  get() 编译时有异常处理， join()编译时无异常处理
 *
 *
 */
public class CompletableFuture05 {
    public static void main(String[] args)  {

        CompletableFuture<String> completableFuture = CompletableFuture.supplyAsync(() -> {
            try {
                Thread.sleep(TimeUnit.SECONDS.toMillis(5));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return "xx";
        });

        try {
            System.out.println(completableFuture.get());
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

        System.out.println(completableFuture.join());

        System.out.println("main end");
    }


}
