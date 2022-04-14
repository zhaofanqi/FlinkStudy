package demo5KafkaAdminClient;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigResource;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;


/**
 *   该种方式仅仅展示 topic  的分配情况，没有展示 topic  的相关配置信息
 */
public class MyDescribeTopic {
    public static void main(String[] args) {

        // 1 创建连接配置
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "tbds-172-16-122-50:9092");

        // 2 创建 KafkaAdminClient 实例 连接kafka 集群
        AdminClient adminClient = KafkaAdminClient.create(properties);

        // 3 借助 KafkaAdminClient 实例 执行操作
        DescribeTopicsResult topicsResult = adminClient.describeTopics(Collections.singleton("zhaofq_topic_client2"));


        KafkaFuture<Map<String, TopicDescription>> future = topicsResult.all();

        try {
            // 异步执行，这里采用  !future.isDone() 让进入这里执行
            while (!future.isDone()) {
                Map<String, TopicDescription> topicDescriptionMap = future.get();
                for (String key : topicDescriptionMap.keySet()) {
                    TopicDescription topicDescription = topicDescriptionMap.get(key);
                    System.out.println(topicDescription);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }




        // 4 关闭 KafkaAdminClient 实例
        adminClient.close();

    }
}
