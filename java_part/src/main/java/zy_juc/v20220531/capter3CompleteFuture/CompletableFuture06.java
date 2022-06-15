package zy_juc.v20220531.capter3CompleteFuture;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * ClassName CompletableFuture06
 *
 * @Auther: 赵繁旗
 * @Date: 2022/6/15 08:45
 * @Description: 电商比价demo：
 * 从三家电商平台同时获取价格
 */
public class CompletableFuture06 {

    static List<NetMall> list = Arrays.asList(
            new NetMall("jd"),
            new NetMall("taobao"),
            new NetMall("dangdang")
    );


    public static void main(String[] args) {
        String prodName = "mysql";
        // 传统的单线程处理
        long start = System.currentTimeMillis();
        List<String> priceList = getPrice(list, prodName);
        for (String element : priceList) {
            System.out.println(element);
        }
        long end = System.currentTimeMillis();
        System.out.println("耗费时间为： \t" + (end - start));

        System.out.println("=================");
        long start2 = System.currentTimeMillis();
        List<String> priceList2 = getCompletableFuturePrice(list, prodName);
        for (String element : priceList2) {
            System.out.println(element);
        }
        long end2 = System.currentTimeMillis();
        System.out.println("耗费时间为： \t" + (end2 - start2));


    }

    public static List<String> getPrice(List<NetMall> list, String prodName) {

        return list.stream().map(netMall ->
                String.format(prodName + " in %s price is %.2f", netMall.getMallName(), netMall.cal(netMall.getMallName(), prodName)))
                .collect(Collectors.toList());
    }


    public static List<String> getCompletableFuturePrice(List<NetMall> list, String prodName) {

        return list.stream().map(netMall -> CompletableFuture.supplyAsync(
                () -> String.format(prodName + "in %s price is %.2f", netMall.getMallName(), netMall.cal(netMall.getMallName(), prodName))
        ))//每个异步任务的工作内容
                .collect(Collectors.toList())
                .stream()
                .map(x -> x.join())// 将Future任务结果获取到
                .collect(Collectors.toList());

    }

}

@AllArgsConstructor
@NoArgsConstructor
@Data
class NetMall {
    private String mallName;

    public Double cal(String mallName, String prodName) {
        try {
            Thread.sleep(TimeUnit.SECONDS.toMillis(2));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        double v = new Random().nextDouble() * 100;
        return v + prodName.charAt(0);
    }
}
