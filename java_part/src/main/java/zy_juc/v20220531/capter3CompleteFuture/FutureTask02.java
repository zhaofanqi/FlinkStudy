package zy_juc.v20220531.capter3CompleteFuture;

import java.util.concurrent.*;

/**
 * ClassName FutureTask02
 *
 * @Auther: 赵繁旗
 * @Date: 2022/6/10 14:29
 * @Description:
 *  与线程池一起使用
 * 验证 get() 阻塞
 * isDone() 空循环占用CPU
 */
public class FutureTask02 {
    public static void main(String[] args) throws InterruptedException, ExecutionException {
        //  3 个任务
        ExecutorService threadPool = Executors.newFixedThreadPool(3);
        long start = System.currentTimeMillis();

       /* Thread.sleep(TimeUnit.SECONDS.toMillis(5));
        Thread.sleep(TimeUnit.SECONDS.toMillis(2));
        Thread.sleep(TimeUnit.SECONDS.toMillis(3));*/

        FutureTask<String> futureTask1 = new FutureTask<>(() -> {
            System.out.println("action task1 ...");
//            Thread.sleep(TimeUnit.SECONDS.toMillis(5));
            Thread.sleep(TimeUnit.SECONDS.toMillis(15));// 一个任务速度慢了，调度线程需要一直等待get
            return "task1 over";
        });
        threadPool.submit(futureTask1);
        FutureTask<String> futureTask2 = new FutureTask<>(() -> {
            System.out.println("action task2 ...");
            Thread.sleep(TimeUnit.SECONDS.toMillis(5));
            return "task2 over";
        });
        threadPool.submit(futureTask2);
        Thread.sleep(TimeUnit.SECONDS.toMillis(5));
        System.out.println(futureTask2.get());
//        System.out.println(futureTask1.get());


        while (true){
            if (futureTask1.isDone()){
                System.out.println(futureTask1.get());
                break;
            }else {
                System.out.println("task1 actioning");
                Thread.sleep(TimeUnit.SECONDS.toMillis(1));
            }
        }

        long end = System.currentTimeMillis();
        System.out.println("execution time :"+(end-start)+"mills ");

        threadPool.shutdown();

    }
}
