package zy_juc.v20220531.capter4;

import java.util.concurrent.TimeUnit;

/**
 * ClassName SynchronizedDemo8
 *
 * @Auther: 赵繁旗
 * @Date: 2022/6/20 15:39
 * @Description:
 *
 * Synchronized
 *              synchronized修饰普通同步方法
 *                  是悲观锁，锁的是当前对象，一个时刻只要一个线程去调用其中一个synchronized方法，
 *                   则只能等待锁的释放，其他线程才能执行synchronized方法,普通方法不受影响
 *              synchronized修饰静态同步方法
 *                  锁的是当前类模板，是类锁.类锁与对象锁是两种不同的对象，所以 静态同步方法与同步方法之间不存在竞争
 *  8锁案例
 *  1 标准访问有 ab 两个线程，先打印邮件还是信息
 *  2 sendEmail 方法中加入暂停3秒，先打印邮件还是短信
 */
class Phone{
    public static synchronized void sendMail(){
        try {
            Thread.sleep(TimeUnit.SECONDS.toMillis(3));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("----sendMail");
    }
    public static synchronized void sendSMS(){
        System.out.println("-----sendSMS");
    }
    public  void hello(){
        System.out.println("----hello");
    }
}
public class SynchronizedDemo8 {
    public static void main(String[] args) {
        Phone phone = new Phone();
        Phone phone2 = new Phone();

        new Thread(()->{
            phone.sendMail();
        },"a").start();
        try {
            Thread.sleep(200);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        new Thread(()->{
//            phone.sendSMS();
            phone2.sendSMS();
//            phone.hello();
        },"b").start();

    }
}
