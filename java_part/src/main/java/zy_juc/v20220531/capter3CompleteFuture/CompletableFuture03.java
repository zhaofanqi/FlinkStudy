package zy_juc.v20220531.capter3CompleteFuture;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * ClassName CompletableFuture
 *
 * @Auther: 赵繁旗
 * @Date: 2022/6/13 17:15
 * @Description:
 *  了解CompeletableFuture四个核心静态方法
 */
public class CompletableFuture03 {
    public static void main(String[] args) throws ExecutionException, InterruptedException {

        // 需要自定义线程池，否则主线程结束时，默认的线程池也会被关闭
        ExecutorService threadPool = Executors.newFixedThreadPool(5);
        CompletableFuture<Void> completableFuture = CompletableFuture.runAsync(() -> {
            System.out.println("coming in ");
            System.out.println(Thread.currentThread().getName());
        },threadPool);
        System.out.println(completableFuture.get());

        CompletableFuture<String> stringCompletableFuture = CompletableFuture.supplyAsync(() -> {
            System.out.println(Thread.currentThread().getName());
            return "hello Supply";
        },threadPool);
        System.out.println(stringCompletableFuture.get());
        threadPool.shutdown();
    }
}
