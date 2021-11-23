package demo2;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class MyProducer {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"tbds-172-16-122-50:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class);
        /**
         *
         * "all", "-1", "0", "1“
         *   all == -1 所有的副本都发送ack
         *   0 :  producer发送消息以后就不管
         *   1 :  topic所在master发送 ack
         */
//        properties.put(ProducerConfig.ACKS_CONFIG,"all");
        KafkaProducer producer = new KafkaProducer<>(properties);
        for (int i=0;i<10;i++){
            producer.send(new ProducerRecord("zfq","发送信息"+i+"\t次"));
        }
        producer.close();
    }
}
