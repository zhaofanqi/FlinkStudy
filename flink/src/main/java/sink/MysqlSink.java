package sink;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * ClassName MysqlSink
 *
 * @Auther: 赵繁旗
 * @Date: 2022/4/20 15:15
 * @Description:    将数据写入mysql
 *
 */
public class MysqlSink {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> clicks = env.readTextFile("./flink/src/main/resources/clicks.txt");


        DataStreamSink mysqlSink = clicks.addSink(JdbcSink.sink("insert into table_name(column_1,column_2) values (?,?)",
                // 自定义类实现 JdbcStatementBuilder
                new MyJdbcStatementBuilder(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://localhost:3306?dbName")
                        .withDriverName("com.mysql.jdbc.Driver")
                        .withUsername("root")
                        .withPassword("123456")
                        .build()
        ));

        env.execute();


    }
    public static class  MyJdbcStatementBuilder implements JdbcStatementBuilder<String>{


        @Override
        public void accept(PreparedStatement preparedStatement, String s) throws SQLException {
//            preparedStatement.setString(1,s);
        }
    }


}
