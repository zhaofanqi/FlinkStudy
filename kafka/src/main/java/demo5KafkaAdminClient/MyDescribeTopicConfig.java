package demo5KafkaAdminClient;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigResource;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 *  adminClient.describeConfigs 展示的是 topic 中的config 信息
 */
public class MyDescribeTopicConfig {
    public static void main(String[] args) {
        // 连接的配置信息
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,"tbds-172-16-122-50:9092");
        // 获取 KafkaAdminClient 实例，连接 kafka 集群
        AdminClient adminClient = KafkaAdminClient.create(properties);
        // 创建 topic  配置信息
        String topic="zhaofq_topic_client2";
        ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, topic);

        // 使用 KafkaAdminClient 实例对象执行相关操作
        DescribeConfigsResult describeConfigsResult = adminClient.describeConfigs(Collections.singleton(configResource));
        KafkaFuture<Map<ConfigResource, Config>> mapKafkaFuture = describeConfigsResult.all();

        try {
            while (!mapKafkaFuture.isDone()){
                Map<ConfigResource, Config> topicConfigMap = mapKafkaFuture.get();
                for (Map.Entry<ConfigResource, Config> configEntry : topicConfigMap.entrySet()) {
//                    System.out.println(topicConfigMap.get(configEntry));  // null
                    System.out.println(configEntry);
                }



            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }finally {
            // 关闭 KafkaAdminClient 实例对象
            adminClient.close();

        }
        // 关闭 KafkaAdminClient 实例对象
        adminClient.close();


    }
}
