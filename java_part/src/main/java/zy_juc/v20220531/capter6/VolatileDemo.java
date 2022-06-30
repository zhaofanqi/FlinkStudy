package zy_juc.v20220531.capter6;

import java.util.concurrent.TimeUnit;

/**
 * ClassName VolatileDemo
 *
 * @Auther: 赵繁旗
 * @Date: 2022/6/30 10:50
 * @Description:
 * 可见性
 * 不保证原子性
 */
public class VolatileDemo {
//    static boolean flag=true;
    static volatile  boolean flag=true;
    public static void main(String[] args) {
//        seeMethod();
        noAtomic();

    }

    private static void noAtomic() {
        Number number = new Number();
        for (int i = 0; i < 10; i++) {
            new Thread(()->{
                for (int j = 0; j < 10000; j++) {
                    number.addPlus();
                }

            },"method"+i).start();
        }
        try {
            Thread.sleep(TimeUnit.SECONDS.toMillis(5));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("最终结果"+number.num);
    }

    private static void seeMethod() {
        //可见性
        new Thread(()->{
            System.out.println("come in");
            while (flag){

            }
            System.out.println(" come out ");
        },"t1").start();
        try {
            Thread.sleep(TimeUnit.SECONDS.toMillis(2));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        flag=false;
        System.out.println("触发停止");
    }
}
class   Number{
    public int num;
    public void addPlus(){
        ++num;
    }
}
