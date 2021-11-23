package demo.CheatCheck;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class TrancationSource implements SourceFunction<Trancation> {

public  AtomicInteger count;
    public int loops=5;

    @Override
    public void run(SourceContext<Trancation> sourceContext) throws Exception {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-mm-dd HH:mm:ss");
        AtomicInteger id=new AtomicInteger(1);
        for (int i = 0; i < loops; i++) {
            System.out.println("=======================");
            count=new AtomicInteger(20);
            while (count.getAndDecrement()>=0){
                Trancation trancation = new Trancation(id.getAndIncrement(), sdf.format(new Date()), new Random().nextDouble() % 100);
                sourceContext.collect(trancation);
            }
            TimeUnit.SECONDS.sleep(2);
        }


    }

    @Override
    public void cancel() {

    }
}
