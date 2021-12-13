package demo5KafkaAdminClient;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.KafkaAdminClient;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class DeleteTopic {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,"tbds-172-16-122-50:9092");
        AdminClient adminClient = KafkaAdminClient.create(properties);

        DeleteTopicsResult deleteTopics = adminClient.deleteTopics(Collections.singletonList("zhaofqTempTopic"));

        try {
            deleteTopics.all().get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }


    }
}
