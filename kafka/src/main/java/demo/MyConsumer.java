package demo;







import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class MyConsumer {
    public static void main(String[] args) {
        Properties properties = new Properties();

        // 消费的kafka 机器，key ,value的反序列化，消费者租，是否自动提交offset,自动提交offset方式
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"tbds-172-16-122-50:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"zhaofq");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"true");
        /**
         *  当消费者读取偏移量无效的情况下，需要重置消费起始位置，默认为latest（从消费者启动后生成的记录），另外一个选项值是 earliest，将从有效的最小位移位置开始消费
         *   earliest:，会从该分区当前最开始的offset消息开始消费(即从头消费)，如果最开始的消息offset是0，那么消费者的offset就会被更新为0.
         *   latest: 只消费当前消费者启动完成后生产者新生产的数据。旧数据不会再消费。offset被重置为分区的HW。
         *   none:  该消费者所消费的主题的分区没有被消费过，就会抛异常
          */


        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        //创建消费者，订阅主题
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String,String>(properties);
        consumer.subscribe(Collections.singleton("zfq"));
        //常驻线程不断消费
        while (true){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record.topic()+"\t"+record.partition()+"\t"+record.value()+"\t"+record.offset());
            }
        }



    }
}
