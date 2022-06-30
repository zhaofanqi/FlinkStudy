package zy_juc.v20220531.capter4;

import java.util.concurrent.TimeUnit;

/**
 * ClassName DeadLockDemo
 *
 * @Auther: 赵繁旗
 * @Date: 2022/6/21 15:01
 * @Description:
 * 死锁：是指两个或者两个以上线程在执行过程中，因争夺资源而造成的一种 互相等待 的现象
 *
 * 死锁代码：
 * 死锁检查：
 * 1、jps -l 得到所有程序的进程号 类锁于 ps -ef |grep
 * 2、 jstack 进程号
 * 死锁检查方式二： jconsole 图形化界面 线程 死锁检查
 *
 */
public class DeadLockDemo {
    public static void main(String[] args) {
        Object A = new Object();
        Object B= new Object();

        new Thread(()->{
            synchronized (A){
                System.out.println(Thread.currentThread().getName()+"\t 持有资源A");
                try {
                    Thread.sleep(TimeUnit.SECONDS.toMillis(1));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                synchronized (B){
                System.out.println(Thread.currentThread().getName()+"\t 想获取资源B");

               }
            }
        },"A").start();

        new Thread(()->{
            synchronized (B){
                System.out.println(Thread.currentThread().getName()+"\t 持有资源B");
                try {
                    Thread.sleep(TimeUnit.SECONDS.toMillis(1));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                synchronized (A){
                    System.out.println(Thread.currentThread().getName()+"\t 想获取资源A");

                }
            }
        },"B").start();

        System.out.println("over");
    }
}
