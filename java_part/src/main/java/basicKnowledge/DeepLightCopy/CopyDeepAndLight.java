package basicKnowledge.DeepLightCopy;

/**
 *  深复制浅复制
 *  当复制的是基本类型时 ，修改任一一个，另外一个也会被影响
 *  当复制的引用类型时，浅复制仅仅复制引用，而深复制会重新开辟内存空间和引用地址 此时复制前后对象独立
 */
public class CopyDeepAndLight   {
    public static void main(String[] args) throws CloneNotSupportedException {


        Deep deep = new Deep("deep",19,new Mid("mid"));
        Deep deep2 = (Deep)deep.clone();
        deep2.setDeepAge(20);
        deep2.getMid().setMidName("secondMid");
        System.out.println("原来对象"+deep);
        System.out.println("修改后对象"+deep2);

        System.out.println("===============");


        Light light = new Light("light", 26,new Mid("mid"));

        Light light2 = (Light)light.clone();
        light2.setLightAge(36);
        light2.getMid().setMidName("secondMid");
        System.out.println("原来对象"+light);
        System.out.println("修改后对象"+light2);



    }
}
