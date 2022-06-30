package zy_juc.v20220531.capter4;

import java.util.concurrent.TimeUnit;

/**
 * ClassName Curi
 *
 * @Auther: 赵繁旗
 * @Date: 2022/6/21 13:35
 * @Description:
 *
 * 可重入锁又称为递归锁 ReentrantLock 和 Synchronized都是可重入锁。
 * 而且Synchronized是隐式锁，ReentrantLock是显示锁。前者不需要自己释放，后者在调用时需要lock unlock数量匹配，否则会出现阻塞
 * 可重复递归调用的锁，在外层使用锁之后，在内层仍然可以使用并且不发生死锁
 * 是指同一个线程在外层方法获取锁的时候，再进入该线程的内层方法时会自动获取锁（前提锁对象是同一把锁），不会因为 之前已经获取过还没释放而阻塞
 * 在一个Synchronized修饰的方法或者代码块的内部调用本类的其他Synchronized修饰的方法或者代码块时，是永远可以获取到锁的
 *
 */

class Phone2{
    public static synchronized void sendMail(){
        try {
            Thread.sleep(TimeUnit.SECONDS.toMillis(2));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println(Thread.currentThread().getName()+"\t"+"----sendMail");
        sendSMS();
    }
    public static synchronized void sendSMS(){
        System.out.println(Thread.currentThread().getName()+"\t-----sendSMS");
    }
    public  void hello(){
        System.out.println("----hello");
    }
}
public class ReEnterLockDemo {
    public static void main(String[] args) {
        Phone2 phone = new Phone2();
        new Thread(()->{
            phone.sendMail();
        },"a").start();
        try {
            Thread.sleep(TimeUnit.MILLISECONDS.toMillis(200));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        new Thread(()->{
            phone.sendMail();
        },"b").start();

    }
}
