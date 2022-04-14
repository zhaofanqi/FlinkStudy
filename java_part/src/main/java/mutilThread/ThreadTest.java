package mutilThread;

import java.util.concurrent.TimeUnit;

public class ThreadTest {
    public static void main(String[] args) {

        long l0 = System.currentTimeMillis();
        Thread t1 = new Thread(()->{
            for (int i = 0; i < 5; i++) {
                try {
                    TimeUnit.SECONDS.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
        t1.start();
        long l1 = System.currentTimeMillis();
        System.out.println("==============="+(l1-l0));
        while (true){
            Thread.State state = t1.getState();
            if (state== Thread.State.TERMINATED){
                break;
            }
        }
        long l2 = System.currentTimeMillis();
        System.out.println("**************"+(l2-l1));
        t1.interrupt();

    }
}
