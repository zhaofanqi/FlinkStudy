package Utils;

import org.apache.kafka.common.TopicPartition;

import java.sql.*;
import java.util.Map;

public class DBUtils {
    public static Connection connection = getMysqlConnection();


    public static Connection getMysqlConnection() {
        Map<String, String> map = FileUtils.getProperties("conf");
        String url = map.get("mysqlUrl");
        String username = map.get("mysqlUser");
        String password = map.get("mysqlPassWord");
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            connection= DriverManager.getConnection(url, username, password);
        } catch (SQLException | ClassNotFoundException throwables) {
            throwables.printStackTrace();
        }
        return connection;
    }

    public static void storeTopicPartitionOffset(TopicPartition topicPartition, long offset) {

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

    /**
     * 获取上次提交的 topic Partition 的 offset
     * @param topicPartition
     * @return
     */
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
    }
    /*public static void main(String[] args) {
        try {
            PreparedStatement preparedStatement = connection.prepareStatement("select count(1) from kafka_topic_partition");
            ResultSet resultSet = preparedStatement.executeQuery();
            while (resultSet.next()){
                System.out.println(resultSet.getInt(1));
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }*/
}
