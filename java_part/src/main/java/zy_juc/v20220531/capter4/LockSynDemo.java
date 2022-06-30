package zy_juc.v20220531.capter4;

/**
 * ClassName LockSynDemo
 *
 * @Auther: 赵繁旗
 * @Date: 2022/6/20 16:49
 * @Description:
 * Synchronized 代码块 ：反编译查看 javap -C xxx.class
 * 可以看到 一个monitorenter 和2个 monitorexit(一个是正常退出释放锁，一个是异常退出释放锁。手动throw时，则为一个monitorexit)
 * Synchronized 方法 反编译查看 javap -v xxx.class flags: ACC_PUBLIC, ACC_SYNCHRONIZED
 * Synchronized 静态方法 反编译查看 javap -v xxx.class  flags: ACC_PUBLIC, ACC_STATIC, ACC_SYNCHRONIZED
 */
public class LockSynDemo {
    Object o=new Object();
    /*public void m1(){
        synchronized (o){
            System.out.println("======m1");
        }
    }
    */
    public synchronized  void m2(){
        System.out.println("=======m2");
    }
    public static synchronized  void m3(){
        System.out.println("=======m3");
    }
    public static void main(String[] args) {

    }
}
