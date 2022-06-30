package zy_juc.v20220531.capter5;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

/**
 * ClassName LockSupportDemo
 *
 * @Auther: 赵繁旗
 * @Date: 2022/6/27 10:54
 * @Description: LockSupport 是一个工具类：线程阻塞和线程释放 unpark()方法凭证，park()消费凭证
 * 线程阻塞与唤醒的三种方式
 * 1、synchronized + wait +notify
 * 2、Lock，condition+await+signal
 * 3、LockSupport.park+LockSupport+unpark
 * 方式一和方式二 有先后执行顺序+锁代码的限制条件
 * LockSupport 没有先后执行顺序：因为采用发放凭证和消费凭证的方式，但是凭证最多只有一个
 * 没有锁代码块的限制条件：因为LockSupport工具类的静态方法提供了线程阻塞和取消线程阻塞
 *
 */
public class LockSupportDemo {
    public static void main(String[] args) {

//        synchronizedDemo();
//        lockDemo();

        Thread t1 = new Thread(() -> {
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println("coming in");
            LockSupport.park();
            System.out.println("coming out");
        }, "t1");
        t1.start();

       /* try {
            TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }*/
        new Thread(()->{
            System.out.println("唤醒");
            LockSupport.unpark(t1);
        },"t2").start();

    }

    private static void lockDemo() {
        ReentrantLock lock = new ReentrantLock();
        Condition condition = lock.newCondition();
        new Thread(()->{
            try {
                lock.lock();
                System.out.println("coming in");
                condition.await();
                System.out.println("coming out");
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                lock.unlock();

            }
        },"t1").start();
        try {
            TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        new Thread(()->{
            try {
                lock.lock();
                System.out.println("唤醒");
                condition.signal();
            } finally {
                lock.unlock();
            }
        },"t2").start();
    }

    private static void synchronizedDemo() {
        Object synObject = new Object();
        new Thread(() -> {
            synchronized (synObject) {
                System.out.println(Thread.currentThread().getName() + "coming in");
                try {
                    synObject.wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                System.out.println(Thread.currentThread().getName() + "come out");
            }
        }, "t1").start();

        try {
            TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        new Thread(() -> {
            synchronized (synObject) {
                System.out.println(Thread.currentThread().getName() + "唤醒");
                synObject.notify();
            }
        }, "t2").start();
    }
}
