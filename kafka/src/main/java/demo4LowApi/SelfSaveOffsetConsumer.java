package demo4LowApi;

import Utils.DBUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.sql.*;
import java.time.Duration;
import java.util.*;

/**
 * 1 按照分区去消费数据
 * 2 从分区指定位置开始消费
 * 3 从分区指定时间戳以后开始消费
 * 4 分区再平衡监听器的使用
 * 5 分区offset自定义存储
 */
public class SelfSaveOffsetConsumer {
    private static Connection connection = DBUtils.getMysqlConnection();

    public static void main(String[] args) {
//        getConn();
//        getPropertiesFile();
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "tbds-172-16-122-50:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "zhaofq");
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10000);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singletonList("zhaofq_test1019"));
        while (true) {
            // 当 拉取的时间1s一次时，控制台可以看出分区存在被多次输出
            ConsumerRecords<String, String> allRecoreds = consumer.poll(Duration.ofSeconds(1));
//            ConsumerRecords<String, String> allRecoreds = consumer.poll(Duration.ofMillis(100));
            // consumer.assignment(); 放在循环内部是应对： 分区再平衡时，可以感知consumer与 topicPartition之间的关系
            // 后续会在 subcribe()中解决该问题
            Set<TopicPartition> assignment = consumer.assignment();
            for (TopicPartition topicPartition : assignment) {
                // 获取 topic partition 的 offset
                Long offset = DBUtils.getTopicPartitionOffest(topicPartition);
                //确定 开始获取数据的位置
//                System.out.println(topicPartition.topic()+" partition= "+topicPartition.partition()+"offset = "+offset);
                consumer.seek(topicPartition, offset);
            }
            System.out.println("============" + allRecoreds.partitions().size());
            for (TopicPartition partition : allRecoreds.partitions()) {
                List<ConsumerRecord<String, String>> partitionRecords = allRecoreds.records(partition);
                for (ConsumerRecord<String, String> partitionRecord : partitionRecords) {
                    if (partitionRecord.value() != null) {
                        System.out.println("topic\t=\t" + partitionRecord.topic() +
                                "\tpartition=\t" + partitionRecord.partition() +
                                "\toffset=\t" + partitionRecord.offset() +
                                "\tvalue=\t" + partitionRecord.value());
                    }
                }
                //消费结束以后，存储 每个 topic partition 的 最大 offset
                if (partitionRecords.size() < 1) {
                    continue;
                } else {
                    //  offset +1 是为了指定下一次消费的offset位置
                    DBUtils.storeTopicPartitionOffset(partition, partitionRecords.get(partitionRecords.size() - 1).offset() + 1);
                }
            }
        }

    }

    /*private static void storeTopicPartitionOffset(TopicPartition topicPartition, long offset) {

        String storeOffsetSql = "insert into kafka_topic_partition(belong_partition,belong_topic,offset) values (?,?,?) ON DUPLICATE KEY UPDATE offset=? ";
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(storeOffsetSql);
            preparedStatement.setString(1, String.valueOf(topicPartition.partition()));
            preparedStatement.setString(2, topicPartition.topic());
            preparedStatement.setLong(3, offset);
            preparedStatement.setLong(4, offset);
            int i = preparedStatement.executeUpdate();
            System.out.println("execute status is " + i);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }

    }


    public  static Long getTopicPartitionOffest(TopicPartition topicPartition) {

        String getOffsetSql = "select  offset from kafka_topic_partition where belong_topic=? and belong_partition=?  ";
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(getOffsetSql);
            preparedStatement.setString(1, topicPartition.topic());
            preparedStatement.setString(2, String.valueOf(topicPartition.partition()));
            ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                return Long.valueOf(resultSet.getInt(1));
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return 0L;
    }*/

    /*private static Connection getConn() {
        Map<String, String> map = getPropertiesFile();
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            connection = DriverManager.getConnection(map.get("mysqlUrl"), map.get("mysqlUser"), map.get("mysqlPassWord"));
            //测试能否查询
            *//*
            String sql = "select count(1) from kafka_topic_partition ";
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                System.out.println(resultSet.getInt(1));
            }
            *//*
            return connection;
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return connection;

    }
*/
    /*private static Map<String, String> getPropertiesFile() {
        Map<String, String> map = new HashMap<>();
        ResourceBundle bundle = ResourceBundle.getBundle("conf");
        Enumeration<String> keys = bundle.getKeys();
        while (keys.hasMoreElements()) {
            String key = keys.nextElement();
            map.put(key, bundle.getString(key));
            System.out.println(key + " = " + bundle.getString(key));
        }
        return map;
    }*/


}
