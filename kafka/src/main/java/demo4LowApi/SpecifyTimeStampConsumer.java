package demo4LowApi;

import kafka.tools.ConsoleConsumer;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;

/**
 * 1 按照分区去消费数据
 * 2 从分区指定位置开始消费
 * 3 从分区指定时间戳以后开始消费
 * 4 分区再平衡监听器的使用
 * 5 分区offset自定义存储
 */
public class SpecifyTimeStampConsumer {

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "tbds-172-16-122-50:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "zhaofq");

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);

        kafkaConsumer.subscribe(Collections.singletonList("zhaofq_test1019"));

        kafkaConsumer.poll(Duration.ofSeconds(5));
        Set<TopicPartition> assignment = kafkaConsumer.assignment();
        System.out.println(assignment.size());

        /**
         *   kafka 消费指定时间以后的消息
         *      构建分区和时间之间的关系
         *      kafkaConsumer绑定这种关系
         *      seek()指定分区与偏移量
         */


        //存储分区与时间关系
        Map<TopicPartition, Long> timeStampsToSearch = new HashMap<>();
        for (TopicPartition topicPartition : assignment) {
//            timeStampsToSearch.put(topicPartition, System.currentTimeMillis()-1*24*60*60*1000);
            System.out.println(topicPartition);
            timeStampsToSearch.put(topicPartition, 1638955120797L);
        }


        /**
         *
         * offsetsForTImes 通过timestamp来查询与此对应的分区位置 ，当对应分区查询结果为null时，该分区不拉取数据
         *                  该方法返回值 OffsetAndTimestamp 封装了 offset与 TimeStamp的关系
         */
        Map<TopicPartition, OffsetAndTimestamp> topicPartitionOffsetAndTimestampMap = kafkaConsumer.offsetsForTimes(timeStampsToSearch);

        System.out.println(topicPartitionOffsetAndTimestampMap.size());

        for (TopicPartition topicPartition : assignment) {
            System.out.println(topicPartition);
            OffsetAndTimestamp offsetAndTimestamp = topicPartitionOffsetAndTimestampMap.get(topicPartition);
            if (offsetAndTimestamp!=null){
                // 设定的时间节点 无producer生产的情况下，只能输出该时间点以后的数据
                System.out.println("partition="+topicPartition.partition()
                        +" partitionTime="+offsetAndTimestamp.timestamp()
                        +" partitionOffset="+offsetAndTimestamp.offset());
                kafkaConsumer.seek(topicPartition, offsetAndTimestamp.offset());
            }else {
                System.out.println("offsetAndTimestamp is null");
            }
        }
        try {
            while (true) {
                ConsumerRecords<String, String> allRecords = kafkaConsumer.poll(Duration.ofSeconds(1));
                for (TopicPartition partition : allRecords.partitions()) {
                    List<ConsumerRecord<String, String>> partitionRecords = allRecords.records(partition);
                    for (ConsumerRecord<String, String> partitionRecord : partitionRecords) {
                        System.out.println("topic=" + partitionRecord.topic()
                                + "\t partition=" + partitionRecord.partition()
                                + "\t offset=" + partitionRecord.offset()
                                + "\t timestamps="+ partitionRecord.timestamp()
                                + "\t timestamps="+ partitionRecord.timestampType()
                                + "\t value=" + partitionRecord.value());
                    }
                }
            }
        } finally {
            kafkaConsumer.close();
        }


    }

}
