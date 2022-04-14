package demo5KafkaAdminClient;

import org.apache.kafka.clients.admin.*;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class DescTempTopic {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,"tbds-172-16-122-50:9092");
        AdminClient adminClient = KafkaAdminClient.create(properties);

        DescribeTopicsResult describeTopics = adminClient.describeTopics(Collections.singleton("zhaofqTempTopic"));

        try {
            Map<String, TopicDescription> topicDescriptionMap = describeTopics.all().get();
            for (String key : topicDescriptionMap.keySet()) {
                System.out.println(key+"\t"+topicDescriptionMap.get(key));
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }


    }
}
