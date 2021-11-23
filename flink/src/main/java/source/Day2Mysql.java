package source;

import entity.Users;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.operators.DataSink;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.MapPartitionOperator;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.sql.Types;

public class Day2Mysql {
    public static void main(String[] args) throws Exception {
        String driver = "com.mysql.jdbc.Driver";
        String url = "jdbc:mysql://localhost:3306/test?useSSL=false";
        String username = "root";
        String password = "123456";

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<Row> input = env.createInput(JDBCInputFormat
                .buildJDBCInputFormat()
                .setDBUrl(url)
                .setDrivername(driver)
                .setUsername(username)
                .setPassword(password)
                .setQuery("select id,username,usersex from users")
                .setRowTypeInfo(new RowTypeInfo(BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO))
                .finish());
        System.out.println(input.count());
        FlatMapOperator<Row, Users> fromMysql = input.flatMap(new FlatMapFunction<Row, Users>() {
            @Override
            public void flatMap(Row row, Collector<Users> collector) throws Exception {
                Users users = new Users((int) row.getField(0), (String) row.getField(1), (String) row.getField(2));
                collector.collect(users);
            }
        });
        fromMysql.print();

        MapPartitionOperator<Users, Row> toMysql = fromMysql.mapPartition(new MapPartitionFunction<Users, Row>() {
            @Override
            public void mapPartition(Iterable<Users> iterable, Collector<Row> collector) throws Exception {
                for (Users users : iterable) {
                    Row row = new Row(3);
                    row.setField(0, users.getId() * 2);
                    row.setField(1, users.getUsername().substring(1, 3));
                    row.setField(2, users.getSex().substring(0, 2));
                    collector.collect(row);
                }
            }
        });
        DataSink<Row> output = toMysql.output(JDBCOutputFormat
                .buildJDBCOutputFormat()
                .setDBUrl(url)
                .setDrivername(driver)
                .setUsername(username)
                .setPassword(password)
                .setSqlTypes(new int[]{Types.INTEGER, Types.VARCHAR, Types.VARCHAR})
                .setQuery("insert into users values(?,?,?)")
                .finish());
        env.execute();
    }


}
