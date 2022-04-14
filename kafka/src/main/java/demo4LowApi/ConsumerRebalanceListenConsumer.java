package demo4LowApi;

import Utils.DBUtils;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;

/**
 *     1 按照分区去消费数据
 *     2 从分区指定位置开始消费
 *     3 从分区指定时间戳以后开始消费
 *  4 分区再平衡监听器的使用 服务器窗口执行 bin/kafka-topic.sh --alter --zookeeper 172.16.122.50:2181 --topic zhaofq_test1019 --partitions 9 可看到对应的日志输出
 *     5 分区offset自定义存储
 */
public class ConsumerRebalanceListenConsumer {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "tbds-172-16-122-50:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "zhaofq");

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);
        //注意这里使用局部变量，方便offset的管理
        Map<TopicPartition, OffsetAndMetadata> currentOffset = new HashMap<>();
        kafkaConsumer.subscribe(Collections.singletonList("zhaofq_test1019"), new ConsumerRebalanceListener() {
            /**
             *  1 停止拉取消息后 分区Rebalance之前
             *  2 一般执行 消费者消费 offset 的提交，避免重复消费的情况
             *
             *  此外注意： 分区再均衡时，消费者是停止拉取消息的
             *  再分区触发：消费者组中的消费者 增加或者减少  分区数量的增删
             *          误判的情况： Coordinator 认为 consumer 宕机了。
             *                  session.timeout.ms      超过该时间长度没有发送心跳则认为该consumer 挂了
             *                  heartbeat.interval.ms  心跳检测时间间隔
             *                  max.poll.interval.ms   poll拉取最大时间间隔，若拉取的消息在该时间范围内无法即使消费则 消费者主动离开消费者组
             *                  max.poll.records       poll每次拉取消息的最大值，设定时应保证在最大拉取时间范围内被处理
             *          所以再平衡的产生判定：
             *                  心跳超时  ：一般设定 session.timeout.ms>= 3*heartbeat.interval.ms 避免网络不稳定的偶发
             *                  消费者超时：max.poll.interval.ms <= session.timeout.ms [消费实现中，单线程消费信息并发送心跳检测]
             * @params partitions 表示再分区之前所得到的分区
             */

            // 停止拉取消息之后 重分区之前执行  此处的partition 为再分区之前得到的分区
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                // 再均衡发生时使用同步提交，正常提交使用异步提交即可
                kafkaConsumer.commitSync(currentOffset);
                System.out.println("begin ConsumerRebalanceListener"+ "重分区前的分区有");
                for (TopicPartition partition : partitions) {
                    System.out.println( partition);
                }

                currentOffset.clear();
            }

            /**
             *  重分区之后 再次拉取消息之前执行 使用临时变量首次调用，变量中值为空哈
             * @param partitions  此处的partition 为再分区之后得到的分区
             */
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                System.out.println("重分区完成后。。。。");
            // 配合 seek 指定每个分区搜索的位置
                for (TopicPartition partition : partitions) {
                    System.out.println(partition);
                    if (currentOffset.size()==0){
                        kafkaConsumer.seek(partition,0);
                    }else {
                        kafkaConsumer.seek(partition,currentOffset.get(partition).offset());
                    }
                }
            }
        });

        //正常的消费逻辑

        try {
            while (true){
                // 1 拉取消息
                ConsumerRecords<String, String> allRecords = kafkaConsumer.poll(Duration.ofSeconds(1));
                // 2 按照分区消费
                for (TopicPartition partition : allRecords.partitions()) {
                    List<ConsumerRecord<String, String>> partitionRecords = allRecords.records(partition);
                    for (ConsumerRecord<String, String> record : partitionRecords) {
                        //process
                        System.out.println("topic \t "+record.topic()+" partition \t "+record.partition()+"offset "+record.offset()+" value "+record.value());
                        //
                        currentOffset.put(new TopicPartition(record.topic(),record.partition()),
                                          new OffsetAndMetadata(record.offset()+1));
                        // 异步提交 offset
                        kafkaConsumer.commitAsync(currentOffset,null);
                    }
                }
            }
        } finally {
            kafkaConsumer.close();
        }


    }
}
