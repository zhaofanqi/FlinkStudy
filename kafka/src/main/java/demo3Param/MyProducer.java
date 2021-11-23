package demo3Param;

import kafka.server.BrokerConfigHandler;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import test.StringTest;

import java.util.Properties;

/**
 * 用于解释说明常用 producer 参数含义
 */
public class MyProducer {
    public static void main(String[] args) {
        Properties properties = new Properties();
        //基础配置
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "tbds-172-16-122-50:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        /**
         * ProducerConfig.ACKS_CONFIG 默认是 -1 ，可选值为 ："all", "-1", "0", "1"
         *                      -1  和 all 表示生产者需要收到 master的所有副本的ack
         *                      1 表示生产者需要收到 master 的 ack ，认为发送成功
         *                      0 表示生产者无需收到 master 的 ack，就认为发送成功
         */
        properties.put(ProducerConfig.ACKS_CONFIG, "-1");
        /**
         * ProducerConfig.CLIENT_ID_CONFIG 用于说明是哪个客户端生产的数据，默认为 “”
         */
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "zhaofq_computer");
        /**
         * ProducerConfig.BATCH_SIZE_CONFIG 发送消息的大小，单位是 16384 bytes(1024*16) 太大浪费内存，太小不断在client 与 server端请求
         */
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384 * 1024);
        /**
         *  ProducerConfig.MAX_REQUEST_SIZE_CONFIG 生产者发送请求的最大值
         */
        properties.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 1024 * 1024);
        /**
         * ProducerConfig.BUFFER_MEMORY_CONFIG 默认值 32 * 1024 * 1024L， 缓存发往server 的record，
         *                      如果发送消息速度大于broker接受速度，超过MAX_BLOCK_MS_CONFIG时间则会报错
         */
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 32 * 1024 * 1024L);
        /**
         * ProducerConfig.LINGER_MS_CONFIG 默认值为0 ，单位为ms，表示有消息就发送
         *                                  设置一个值后，会延迟发送
         *
         *                                  TODO 消息发送控制：延迟时间，发送消息的最大值是否达到[明天测试]
         */
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 10 * 1000);
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        String line = "";
//        for (int i=1;i<=16;i++){
        line = StringTest.specifyLengthString(1, 'c');
//        }
        // 构建发送消息大小，consumer测试消费次数
        int count = 0;

        while (true) {
            try {
                Thread.sleep(1000 * 2);
                producer.send(new ProducerRecord<>("zfq", line));
                System.out.println("发送次数" + (count++));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if (count > 20) {
                break;
            }
        }
        // TODO 无 producer.close(),消费者无法消费该记录，需要定位原因
//        producer.close();
    }

}
