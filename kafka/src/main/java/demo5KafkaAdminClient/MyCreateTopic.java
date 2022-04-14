package demo5KafkaAdminClient;

import org.apache.kafka.clients.admin.*;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class MyCreateTopic {
    public static void main(String[] args) {

        // 基本配置信息
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,"tbds-172-16-122-50:9092");

        // 创建 KafkaAdminClient 实例 连接kafka集群
        AdminClient adminClient = KafkaAdminClient.create(properties);


        // 构建 topic
        NewTopic addTopic = new NewTopic("zhaofq_topic_client2", 3, (short) 3);
        //构建待创建topic的相关配置
        Map<String, String> map = new HashMap<>();
        map.put("cleanup.policy","compact");
        // 绑定 topic 与 配置
        addTopic.configs(map);

        // 客户端执行操作
        CreateTopicsResult topicsResult = adminClient.createTopics(Collections.singleton(addTopic));

        try {
            topicsResult.all().get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        adminClient.close();

    }
}
