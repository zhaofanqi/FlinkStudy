package zy_juc.v20220531.capter3CompleteFuture;

import java.util.concurrent.*;

/**
 * ClassName CompletableFutureAPIDemo07
 *
 * @Auther: 赵繁旗
 * @Date: 2022/6/15 11:03
 * @Description: 获取结果的几种方式
 * 对得到的结果进行处理
 */
public class CompletableFutureAPIDemo08 {
    public static void main(String[] args) throws InterruptedException, ExecutionException {
        ExecutorService executorService = Executors.newFixedThreadPool(3);

        CompletableFuture.supplyAsync(() -> {
            return 1;
        }, executorService)
                .thenApply(f -> {
                    return f + 2;
                }).thenApply(f -> {
            return f + 3;
        }).thenApply(f -> {
            return f + 4;
        }).whenComplete((v, e) -> {
            if (e == null) {
                System.out.println(" then apply final result: \t" + v);
            }
        }).exceptionally(e -> {
            System.out.println(e.getMessage() + e.getCause());
            return null;
        });

        System.out.println("======");
        CompletableFuture.supplyAsync(() -> {
            return 1;
        }).handle((v, e) -> {
            return v + 2;
        }).handle((v, e) -> {
            return v + 3;
        }).whenComplete((v, e) -> {
            if (e == null) {
                System.out.println(" then handle final result: \t" + v);
            }
        }).exceptionally(e->{
            System.out.println(e.getCause()+e.getMessage());
            return null;
        });

        System.out.println("************");


        // 得到结果以后，进行处理
        System.out.println(CompletableFuture.supplyAsync(() -> {
            return 1;
        }).thenRun(() -> {
        }).join());

        System.out.println(CompletableFuture.supplyAsync(() -> {
            return 1;
        }).thenAccept(v -> {
            System.out.println("then Accept \t" + Thread.currentThread().getName() + "\t" + v);
        }).join());

        System.out.println(CompletableFuture.supplyAsync(() -> {
            return 1;
        }).thenApply(v -> {
            System.out.println(v);
            return v;
        }).join());


        executorService.shutdown();
        System.out.println("main end");
    }
}
