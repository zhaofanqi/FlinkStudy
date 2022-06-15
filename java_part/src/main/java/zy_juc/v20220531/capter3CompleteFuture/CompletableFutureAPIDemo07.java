package zy_juc.v20220531.capter3CompleteFuture;

import java.util.concurrent.*;

/**
 * ClassName CompletableFutureAPIDemo07
 *
 * @Auther: 赵繁旗
 * @Date: 2022/6/15 11:03
 * @Description:    获取结果的几种方式
 * get() 阻塞
 * get(超时时间)
 * join() 阻塞
 * getNow() 立刻获取，否则使用默认值
 * complete() 判断是否打断任务，并直接使用默认值
 */
public class CompletableFutureAPIDemo07 {
    public static void main(String[] args) throws InterruptedException, ExecutionException {
        ExecutorService executorService = Executors.newFixedThreadPool(3);

        CompletableFuture<String> completableFuture = CompletableFuture.supplyAsync(() -> {
            try {
                Thread.sleep(TimeUnit.SECONDS.toMillis(2));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return "a";
        },executorService);

//        completableFuture.get();
        Thread.sleep(TimeUnit.SECONDS.toMillis(3));
//        try {
//            completableFuture.get(1,TimeUnit.SECONDS);
//        } catch (TimeoutException e) {
//            e.printStackTrace();
//        }
//        System.out.println(completableFuture.getNow("b"));
//        completableFuture.join();

        System.out.println(completableFuture.complete("b")+"\t"+completableFuture.get());

        executorService.shutdown();
        System.out.println("main end");
    }
}
