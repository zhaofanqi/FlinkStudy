package demo5KafkaAdminClient;

import org.apache.kafka.clients.admin.*;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class CreateTempTopic {
    public static void main(String[] args) {
        // 连接配置
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,"tbds-172-16-122-50:9092");
        // 创建 KafkaAdminClient 实例，用于操作
        AdminClient client = KafkaAdminClient.create(properties);
        NewTopic topic = new NewTopic("zhaofqTempTopic", 10, (short) 3);
        Map<String, String> config = new HashMap<String,String>();
        config.put("cleanup.policy","compact");
        topic.configs(config);


        CreateTopicsResult result = client.createTopics(Collections.singleton(topic));
        try {
            result.all().get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        client.close();


    }
}
