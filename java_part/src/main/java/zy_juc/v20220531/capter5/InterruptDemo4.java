package zy_juc.v20220531.capter5;

/**
 * ClassName InterrupDemo4
 *
 * @Auther: 赵繁旗
 * @Date: 2022/6/23 14:01
 * @Description:
 * Interrupted 的使用
 * Interrupted 底层会判断中断标识位是否为true/false,之后清除中断标识位
 */
public class InterruptDemo4 {
    public static void main(String[] args) throws InterruptedException {

        Thread.interrupted();
        System.out.println(Thread.currentThread().isInterrupted());//false
        Thread.interrupted();
        System.out.println(Thread.currentThread().isInterrupted());//false
        Thread.currentThread().interrupt();
        System.out.println(Thread.currentThread().isInterrupted());//true
        System.out.println( Thread.interrupted());//true
        System.out.println(  Thread.interrupted());//false


    }
}
