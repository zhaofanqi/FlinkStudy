package demo4LowApi;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.*;

/**
 *     1 按照分区去消费数据
 *     2 从分区指定位置开始消费
 *     3 从分区指定时间戳以后开始消费
 *  4 分区再平衡监听器的使用
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
             * @params partitions 表示再分区之前所得到的分区
             */

            //一般执行
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                // 再均衡发生时使用同步提交，正常提交使用异步提交即可
                kafkaConsumer.commitSync();
            }

            /**
             *  重分区之后 再次拉取消息之前执行
             * @param partitions
             */
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            // 配合 seek 指定每个分区搜索的位置
                for (TopicPartition partition : partitions) {
//                    kafkaConsumer.seek(partition,);
                }
            }
        });

    }
}
