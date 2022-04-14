package demo5KafkaAdminClient;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.common.KafkaFuture;

import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class ListTempTopic {
    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,"tbds-172-16-122-50:9092");

        AdminClient adminClient = KafkaAdminClient.create(properties);

        ListTopicsResult listTopicsResult = adminClient.listTopics();
        try {
            Set<String> topicNames = listTopicsResult.names().get();

            for (String topicName : topicNames) {

                System.out.println(topicName);
            }

        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }


    }
}
