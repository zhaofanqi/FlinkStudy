package zy_juc.v20220531.capter4;

import java.util.concurrent.locks.ReentrantLock;

/**
 * ClassName ReentrantLockDemo
 *
 * @Auther: 赵繁旗
 * @Date: 2022/6/21 08:57
 * @Description: ReentrantLock分公平锁和非公平锁
 * 公平锁：按照线程申请锁的顺序获取锁
 * 非公平锁：多个线程申请锁的时候，并非按照顺序获取锁，有可能造成优先级翻倍或者饥饿现象（ 获取不到锁）
 *
 * 为什么有公平锁和非公平锁之分？默认的是非公平锁原因
 * 非公平锁的效率更高！
 * 原因：1、恢复挂起的线程到真正获取锁中间有时间差，非公平锁能更好的利用CPU的时间片，减少CPU 的空闲时间
 * 2、使用多线程很重要的考量点是线程切换带来的开销。当使用非公平锁的时候，一个线程获取锁得到同步状态，然后释放同步状态
 * 所以刚刚释放锁的线程有大概率再次获取到锁，就减少了线程的开销
 *
 */
class Ticket {
    private int num = 50;
    ReentrantLock lock = new ReentrantLock(true);

    public void sale() {

        try {
            lock.lock();
            if (num>0){
                System.out.println(Thread.currentThread().getName()+"卖出票，剩余票输为：\t"+(num--));
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            lock.unlock();
        }
    }
}

public class ReentrantLockDemo {

    public static void main(String[] args) {
        Ticket ticket = new Ticket();
        new Thread(()->{
            for (int i = 0; i < 50; i++) {
                ticket.sale();
            }
        },"A").start();

        new Thread(()->{
            for (int i = 0; i < 50; i++) {
                ticket.sale();
            }
        },"B").start();

        new Thread(()->{
            for (int i = 0; i < 50; i++) {
                ticket.sale();
            }
        },"C").start();

    }
}
