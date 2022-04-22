package sink;

import entity.Click;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
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

        SingleOutputStreamOperator<Click> clicks = env.readTextFile("./flink/src/main/resources/clicks.txt")
                .map(x -> {
                    String[] fields = x.split(",");
                    return new Click(fields[0], fields[1], Long.parseLong(fields[2]));
                });



        clicks.addSink(JdbcSink.sink("insert into table_name(user,url) values (?,?)",
                // 自定义类实现 JdbcStatementBuilder
                ((statement,click)->{
                    statement.setString(1,click.user);
                    statement.setString(2,click.url);
                }),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://localhost:3306?dbName")
                        .withDriverName("com.mysql.jdbc.Driver")
                        .withUsername("root")
                        .withPassword("123456")
                        .build()
        ));

        env.execute();


    }

}
