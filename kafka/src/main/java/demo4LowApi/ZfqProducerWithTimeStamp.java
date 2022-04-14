package demo4LowApi;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Date;
import java.util.Properties;
import java.util.Timer;

public class ZfqProducerWithTimeStamp {
    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "tbds-172-16-122-50:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "zfq");

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);


        try {
            for (int i = 0; i < 2; i++) {
                long timestamp = new Date().getTime() ;
                System.out.println(timestamp);
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>("zhaofq_test1019",
                                                                                            i % 9,
                                                                                            timestamp,
                                                                                            String.valueOf(i % 9),
                                                                                            "withtimeStamp");
                kafkaProducer.send(producerRecord);
            }
        } finally {
            kafkaProducer.close();
        }


    }
}
