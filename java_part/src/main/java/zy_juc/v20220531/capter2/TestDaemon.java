package zy_juc.v20220531.capter2;

import java.util.concurrent.TimeUnit;

/**
 * ClassName TestDaemon
 *
 * @Auther: 赵繁旗
 * @Date: 2022/6/10 09:59
 * @Description:    守护线程伴随用户线程的结束而结束
 */
public class TestDaemon {
    public static void main(String[] args) {
        Thread t1 = new Thread(() -> {
            while (true){

            }
        }, "t1");
        t1.setDaemon(true);// 设置为守护线程，不设置的情况下，主线程结束，程序也不结束，因为还有用户线程运行
        t1.start();
        System.out.println(t1.isDaemon()?"守护线程":"用户线程");
        try {
            Thread.currentThread().sleep(TimeUnit.SECONDS.toMillis(3));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println(Thread.currentThread().getName()+" \t 结束");
    }
}
