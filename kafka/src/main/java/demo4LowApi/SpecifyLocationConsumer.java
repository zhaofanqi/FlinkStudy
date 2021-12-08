package demo4LowApi;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 1 按照分区去消费数据
 * 2 从分区指定位置开始消费
 * 3 从分区指定时间戳以后开始消费
 * 4 分区再平衡监听器的使用
 * 5 分区offset自定义存储
 */
public class SpecifyLocationConsumer {
    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "tbds-172-16-122-50:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "zhaofq");
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);
        kafkaConsumer.subscribe(Collections.singletonList("zhaofq_test1019"));

        // 完成分区分配 注意这里poll的时间稍微大一点点，否则 poll 命令没执行完任务分配
        kafkaConsumer.poll(Duration.ofSeconds(5));
        Set<TopicPartition> assignPartition = kafkaConsumer.assignment();
        for (TopicPartition topicPartition : assignPartition) {
            // 实现不同分区从不同位置开始消费
            if (topicPartition.partition()<2){
                kafkaConsumer.seek(topicPartition, 550);
            }else{
                kafkaConsumer.seek(topicPartition, 1);
            }
        }

        try {
            while (true) {
                ConsumerRecords<String, String> allRecords = kafkaConsumer.poll(Duration.ofSeconds(1));
                for (TopicPartition partition : allRecords.partitions()) {
                    List<ConsumerRecord<String, String>> partitionRecords = allRecords.records(partition);
                    for (ConsumerRecord<String, String> partitionRecord : partitionRecords) {
                        System.out.println("topic=" + partitionRecord.topic()
                                + "\tpartition=" + partitionRecord.partition()
                                + "\toffset=" + partitionRecord.offset()
                                + "\tvalue=" + partitionRecord.value());
                    }
                }
            }
        } finally {
            kafkaConsumer.close();
        }

    }
}
