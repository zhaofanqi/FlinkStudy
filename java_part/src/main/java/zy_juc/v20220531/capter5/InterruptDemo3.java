package zy_juc.v20220531.capter5;

/**
 * ClassName InterruptDemo3
 *
 * @Auther: 赵繁旗
 * @Date: 2022/6/23 09:24
 * @Description:
 * 当一个线程阻塞调用wait join sleep 等方法，当被其他线程调用interrupt() 时，那么它的中断标识将被清除，并抛出异常
 */
public class InterruptDemo3 {
    public static void main(String[] args) {

        Thread t1 = new Thread(() -> {
            while (true) {
                if (Thread.currentThread().isInterrupted()) {
                    System.out.println(Thread.currentThread().getName() + "\t标识位\t" + Thread.currentThread().isInterrupted());
                    break;
                }

 //当线程处于睡眠状态，线程中断标识位被设置为true,则抛出异常，清除中断标识状态true，恢复默认值，程序陷入无限循环
                try {
                    Thread.sleep(200);
                } catch (InterruptedException e) {
                    // catch 语句中捕获异常处理，可以正常结束了
                    Thread.currentThread().interrupt();
                    e.printStackTrace();
                }
                System.out.println("coming in interrupt");
            }
        }, "t1");
        t1.start();

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        new Thread(() -> t1.interrupt(), "t1").start();


    }
}
