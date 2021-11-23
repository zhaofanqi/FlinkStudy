package source.mysql;


import entity.Users;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.operators.*;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.sql.Types;

public class WihtMysql {
    public static void main(String[] args) throws Exception {
        String url = "jdbc:mysql://localhost:3306/test?useSSL=false";
        String password = "123456";
        String username = "root";
        String driver = "com.mysql.jdbc.Driver";
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        String sqlRead = "select id,username,usersex  from users";
        String writeRead = "insert into users values(?,?,?)";

        DataSource<Row> input = env.createInput(JDBCInputFormat.buildJDBCInputFormat()
                .setDBUrl(url).setDrivername(driver)
                .setUsername(username).setPassword(password)
                .setQuery(sqlRead)
                .setRowTypeInfo(new RowTypeInfo(BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO))
               .finish());

        System.out.println("test 输出"+input.count());
        FlatMapOperator<Row, Users> fromMysqlUsers = input.flatMap(new FlatMapFunction<Row, Users>() {
            @Override
            public void flatMap(Row row, Collector<Users> collector) throws Exception {
                Users users = new Users((int) row.getField(0), (String) row.getField(1), (String) row.getField(2));
              collector.collect(users);
            }
        });
        fromMysqlUsers.print();


        //往mysql写入数据
        MapOperator<Users, Row> toMysql = fromMysqlUsers.map(new MapFunction<Users, Row>() {
            @Override
            public Row map(Users users) throws Exception {
                Row row = new Row(3);
                row.setField(0, users.getId()+10);
                row.setField(1, users.getUsername()+"name");
                row.setField(2, users.getSex()+"x");
                return row;
            }
        });


        toMysql.output(JDBCOutputFormat.buildJDBCOutputFormat()
                .setDBUrl(url).setDrivername(driver)
                .setUsername(username).setPassword(password)
                .setQuery(writeRead)
                .setSqlTypes(new int[]{Types.INTEGER, Types.VARCHAR, Types.VARCHAR})
                .finish());

       env.execute();


    }
}
