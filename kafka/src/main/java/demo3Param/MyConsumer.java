package demo3Param;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;

/**
 *  Kafka Consumer常用参数说明
 *  kafka Consumer消费者的分区分配规则：
 *              1 RangeAssigor  机制：每个topic的分区按照 消费者个数，进行分配
 *                              弊端：每次按照topic的分区数量除以消费者的数量进行分配，当topic的数量较大时，consumer消费的分区数存在较大差异
 *              2 RoundAssigor  机制：将所有的topic的分区排序，订阅者排序，按顺序分别下发
 *                          当存在Consumer的增删时，分区在Consumer中需要重新分配，分区迁移较多
 *              3 StickyAssigor  机制：消费者之间分区最多相差一个；当存在Consumer增删时，尽可能少的进行分区迁移
 *                               与RoundAssigor之间的差距： 消费者的增删，引起分区迁移更少
 *
 *
 *
 */
public class MyConsumer {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "kafkaParam2");
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "tbds-172-16-122-50:9092");
        /**
         * ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG:  默认值为 true  是否自己手动提交 offset
         *                      含义： 高级api 低级api 区别有： 是否手动提交 消费的 offset
         *
         */
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        /**
         * ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, 默认值为  "latest"
         *      可选值："latest" "earliest" "none"
         *        "latest"  当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，消费新产生的该分区下的数据
         *        "earliest" 当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，从头开始消费
         *        "none" topic各分区都存在已提交的offset时，从offset后开始消费；只要有一个分区不存在已提交的offset，则抛出异常
         */
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        KafkaConsumer kafkaConsumer = new KafkaConsumer<>(properties);

        kafkaConsumer.subscribe(Collections.singletonList("zhaofq_test1019"));
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");


        while (true){
            ConsumerRecords records = kafkaConsumer.poll(Duration.ofSeconds(1));
            for (Object record : records) {
                System.out.println("当前时间"+ sdf.format(new Date())+"\t"+record.toString());
            }

            kafkaConsumer.commitAsync(new OffsetCommitCallback() {
                @Override
                public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                    if (exception!=null){
                        System.out.println(" commit offset failed for "+exception);
                    }
                    Set<TopicPartition> topicPartitions = offsets.keySet();
                    for (TopicPartition topicPartition : topicPartitions) {
                        OffsetAndMetadata offsetAndMetadata = offsets.get(topicPartition);

                        System.out.println("topic Partitions is "+topicPartition+"\t   offset is "+offsetAndMetadata.offset()+"\t metaData is "+offsetAndMetadata.metadata());
                    }
                }
            });
        }

    }
}
