package zy_juc.v20220531.capter5;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * ClassName InterruptDemo
 *
 * @Auther: 赵繁旗
 * @Date: 2022/6/22 15:55
 * @Description:
 *
 */
public class InterruptDemo2 {
    static  volatile boolean isStop=false;
    static AtomicBoolean running=new AtomicBoolean(false);
    public static void main(String[] args) {


        Thread t3 = new Thread(() -> {
            for (int i = 0; i < 400; i++) {
                System.out.println("========="+i);
            }
            System.out.println("中断标识03"+Thread.currentThread().isInterrupted());

        }, "t3");
        t3.start();
        try {
            Thread.sleep(TimeUnit.MILLISECONDS.toMillis(2));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("中断标识01"+t3.isInterrupted());
        t3.interrupt();
        //线程中断被设置为true,可程序仍然输出：说明线程中断不会有影响，需要线程自己去配合
        System.out.println("中断标识02"+t3.isInterrupted());



        try {
            Thread.sleep(TimeUnit.MILLISECONDS.toMillis(2000));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        // 此时 中断标识为false，说明对于不活动的线程，不会有任何影响
        System.out.println("中断标识04"+t3.isInterrupted());
    }


}
