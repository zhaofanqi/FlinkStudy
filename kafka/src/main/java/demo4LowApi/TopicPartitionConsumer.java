package demo4LowApi;

import kafka.consumer.SimpleConsumer;
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

/**
 *     1 按照分区去消费数据
 *     2 从分区指定位置开始消费
 *     3 从分区指定时间戳以后开始消费
 *     4 分区再平衡监听器的使用
 *     5 分区offset自定义存储
 */
public class TopicPartitionConsumer {

    public static void main(String[] args) {
boolean isRunning=true;
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"tbds-172-16-122-50:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"zhaofq");
        KafkaConsumer consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singletonList("zhaofq_test1019"));
        while (isRunning){
            ConsumerRecords allRecords = consumer.poll(Duration.ofSeconds(2));
            //  assignment()用于得到消费者消费的分区
            Set<TopicPartition> assignment = consumer.assignment();
            for (TopicPartition topicPartition : assignment) {
                // records(topicPartition) 用于得到指定topic指定partition 的信息
                List<ConsumerRecord> partitionRecord = allRecords.records(topicPartition);
                for (ConsumerRecord consumerRecord : partitionRecord) {
                    System.out.println(" topic = "+consumerRecord.topic()+"; partition = "+consumerRecord.partition()+"  ;offset = "+consumerRecord.offset()+" value = "+consumerRecord.value());
                }
            }
        }
    }
}
