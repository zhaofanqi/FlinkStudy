package zy_juc.v20220531.capter3CompleteFuture;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * ClassName CompletableFutureAPIDemo09
 *
 * @Auther: 赵繁旗
 * @Date: 2022/6/15 14:09
 * @Description:
 */
public class CompletableFutureAPIDemo09 {
    public static void main(String[] args) {
        // 去运行结果最快的 either

        CompletableFuture<String> future1 = CompletableFuture.supplyAsync(() -> {
            try {
                Thread.sleep(TimeUnit.SECONDS.toMillis(2));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return "paly A";
        });
        CompletableFuture<String> future2 = CompletableFuture.supplyAsync(() -> {
            try {
                Thread.sleep(TimeUnit.SECONDS.toMillis(1));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return "play B ";
        });

        CompletableFuture<String> result = future1.applyToEither(future2, f -> {
            return f + " is winner";
        });
        System.out.println(Thread.currentThread().getName()+"\t"+result.join());

        // 等待执行完再统一计算的 combine
        CompletableFuture<Integer> a = CompletableFuture.supplyAsync(() -> {
            return 10;
        });
        CompletableFuture<Integer> b = CompletableFuture.supplyAsync(() -> {
            return 20;
        });
        CompletableFuture<Integer> resultCombine = a.thenCombine(b, (x, y) -> {
            return x + y;
        });
        System.out.println("combine "+resultCombine.join());

        CompletableFuture<Integer> resultDoublCombibe = CompletableFuture.supplyAsync(() -> {
            return 100;
        }).thenCombine(CompletableFuture.supplyAsync(() -> {
            return 200;
        }), (x, y) -> {
            return x + y;
        }).thenCombine(CompletableFuture.supplyAsync(() -> {
            return 1000;
        }), (c, d) -> {
            return c + d;
        });
        System.out.println("Double Combine\t"+resultDoublCombibe.join());


    }
}
