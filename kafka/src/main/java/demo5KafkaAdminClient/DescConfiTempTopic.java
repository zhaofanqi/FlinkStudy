package demo5KafkaAdminClient;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.config.ConfigResource;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class DescConfiTempTopic {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,"tbds-172-16-122-50:9092");
        AdminClient adminClient = KafkaAdminClient.create(properties);
        ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, "zhaofqTempTopic");
        DescribeConfigsResult descConfig = adminClient.describeConfigs(Collections.singleton(configResource));
        try {
            Map<ConfigResource, Config> configResourceConfigMap = descConfig.all().get();
            for (ConfigResource resource : configResourceConfigMap.keySet()) {
                System.out.println(configResourceConfigMap.get(resource));
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

    }
}
