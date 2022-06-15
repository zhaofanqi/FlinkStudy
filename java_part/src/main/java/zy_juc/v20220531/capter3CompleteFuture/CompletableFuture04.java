package zy_juc.v20220531.capter3CompleteFuture;

import java.util.concurrent.*;

/**
 * ClassName CompletableFuture
 *
 * @Auther: 赵繁旗
 * @Date: 2022/6/13 17:15
 * @Description:
 *  了解CompeletableFuture 阻塞问题和不断轮询问题的解决
 */
public class CompletableFuture04 {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
//        selfBasicFunction();

        // 功能强大在：当任务结束时主动上报，而且任务可以配置先后顺序
        ExecutorService threadPool = Executors.newFixedThreadPool(5);
        CompletableFuture.supplyAsync(()->{
            System.out.println("has comed in ");
            try {
                Thread.sleep(TimeUnit.SECONDS.toMillis(4));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            int i=10/0;
            return "compute finish";
        },threadPool).whenComplete((v,e)->{
            if (e==null){// 要不失败的时候也进来了
            System.out.println(Thread.currentThread().getName()+"\t"+"得到完成的结果"+"\t"+v);
            }
        }).exceptionally(e->{
            e.printStackTrace();
            System.out.println(e.getCause()+"\t"+e.getMessage());
            return null;
        });
        System.out.println("main自己在忙");
        threadPool.shutdown();

    }

    private static void selfBasicFunction() throws InterruptedException, ExecutionException {
        // 证明可以实现future的功能
        CompletableFuture<String> future = CompletableFuture.supplyAsync(() -> {
            System.out.println("coming in ");
            try {
                Thread.sleep(TimeUnit.SECONDS.toMillis(5));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return "action start";
        });
        System.out.println(Thread.currentThread().getName()+"\t"+"main继续干活");
        System.out.println("获取处理结果"+future.get());
        while (true){
          if (future.isDone()){
              System.out.println("action finish");
              break;
          }
        }
        System.out.println("end");
    }
}
