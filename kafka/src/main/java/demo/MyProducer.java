package demo;

import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 *  kafka 的生产者连接需要：
 *          连接参数：
 *                  server.id
 *                  key的序列化
 *                  value的序列化
 *                  消息发送
 *
 */
public class MyProducer {
    public  static  int count=5;
    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", "tbds-172-16-122-50:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = new KafkaProducer<>(props);
        JSONObject json = new JSONObject();
        json.put("create_date","20211019");
        json.put("task_node","raw");
        while (count>0) {
            Thread.sleep(2*1000);
            String jsonData;
            producer.send(new ProducerRecord<>("zfq",json.toString()));
            count--;
            System.out.println("发送成功，还剩 "+count+" 次");
        }
        producer.close();
    }
}
