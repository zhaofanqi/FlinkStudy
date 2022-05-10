package UDFunction;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * ClassName StartDemo
 *
 * @Auther: 赵繁旗
 * @Date: 2022/5/10 11:12
 * @Description:        UDF调用方式
 *                          函数要自定义实现对应类型：标量函数 表函数 聚合函数 标量聚合函数 必须编写public eval()方法
 *                          tableAPI中表对象调用
 *                          SQL 中注册使用
 */
public class StartDemo {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //生成表
        String clickDDL="create table clickTable (" +
                "user_name String," +
                "url String ," +
                "ts BIGINT," +
                "et as to_timestamp(from_unixtime(ts/1000))," +
                "WATERMARK FOR et AS et - interval '5' second" +
                ") with (" +
                " 'connector'='filesystem'," +
                "'path'='flink/src/main/resources/clicks.txt'," +
                "'format'='csv'" +
                ")";
        tableEnv.executeSql(clickDDL);
       /* Table countTable  = tableEnv.sqlQuery("select count(1) from clickTable ");
        tableEnv.toChangelogStream(countTable).print("count");*/
        //注册函数
        tableEnv.createTemporarySystemFunction("myHash",MyHash.class);
        // 执行查询  , 转成 流输出
        Table hashTable = tableEnv.sqlQuery("select user_name, myHash(user_name) from clickTable limit 2");
        tableEnv.toDataStream(hashTable).print("hashTable");

        // 使用表对象去查询
        Table clickTable = tableEnv.from("clickTable");
        Table hashTable2 = clickTable.select($("user_name"), call(MyHash.class, $("user_name"))).limit(2);
        Table hashTable3 = clickTable.select($("user_name"), call("myHash", $("user_name"))).limit(2);
        tableEnv.toDataStream(hashTable2).print("hashTable2");
        tableEnv.toDataStream(hashTable3).print("hashTable2");
        env.execute();


    }
    public static class MyHash extends ScalarFunction{
        public int eval(String str){
            return str.hashCode();
        }
    }
}
