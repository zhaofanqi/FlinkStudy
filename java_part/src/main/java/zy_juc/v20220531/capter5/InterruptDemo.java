package zy_juc.v20220531.capter5;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * ClassName InterruptDemo
 *
 * @Auther: 赵繁旗
 * @Date: 2022/6/22 15:55
 * @Description:
 * 中断是一种协商机制，设置中断标识位不代表线程被停止
 * interrupt(): 将线程标识位设置为true
 * isInterrupted() ：判断线程是否有中断标识
 * interrupted():判断线程标识位，并清除线程一些绑定信息
 * 线程中断：一个线程的标识位被设置为中断，此时该线程不会真的被中断，还需要被调用的线程自己配合去执行相关逻辑
 * 对于不活动的线程，不会产生任何影响
 * 通过其他线程中断某个线程的三种方式 volatile atomicBoolean   interrupt+isInterrupted()
 *
 */
public class InterruptDemo {
    static  volatile boolean isStop=false;
    static AtomicBoolean running=new AtomicBoolean(false);
    public static void main(String[] args) {

//        m1_volatile();
//        m2_atomicBoolean();

        Thread t3 = new Thread(() -> {
            while (true) {
                if (Thread.currentThread().isInterrupted()) {
                    System.out.println(Thread.currentThread().getName() + "被中断");
                   break;
                }
                System.out.println("interrupt");
            }
        }, "t3");
        t3.start();
        try {
            Thread.sleep(TimeUnit.MICROSECONDS.toMillis(10));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

//      自己打断自己
//        t3.interrupted();
      /*  new Thread(()->{
            t3.interrupt();
        },"t4").start();
*/


    }

    private static void m2_atomicBoolean() {
        new Thread(()->{
            while (true){
                if (running.get()){
                    System.out.println(Thread.currentThread().getName()+"被中断");
                    break;
                }
                System.out.println("AtomicBoolean");
            }
        },"t1").start();
        try {
            Thread.sleep(TimeUnit.MICROSECONDS.toMillis(10));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        new Thread(()->{
            running.set(true);
        },"t2").start();
    }

    private static void m1_volatile() {
        new Thread(()->{
            while (true){
                if (isStop){
                    System.out.println(Thread.currentThread().getName()+"被中断");
                    break;
                }
                System.out.println("volatile");
            }
        },"t1").start();
        try {
            Thread.sleep(TimeUnit.MICROSECONDS.toMillis(10));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        new Thread(()->{
            isStop=true;
        },"t2").start();
    }
}
