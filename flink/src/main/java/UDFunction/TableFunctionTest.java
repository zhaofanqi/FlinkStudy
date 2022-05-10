package UDFunction;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;

/**
 * ClassName TableFunctionTest
 *
 * @Auther: 赵繁旗
 * @Date: 2022/5/10 14:16
 * @Description: 将 url 的字段进行拆分，实现 lateral view 类型功能的 lateral table .lateral table 是侧输出表 as T(字段名称) 取别名
 */
public class TableFunctionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 创建表并生成
        String clickDDL = "create table clickTable (" +
                "user_name String ," +
                "url String," +
                "ts BIGINT," +
                "et AS to_timestamp(from_unixtime(ts/1000))," +
                "WATERMARK FOR et as et - interval '1' second " +
                ") with (" +
                " 'connector'='filesystem'," +
                " 'path'='flink/src/main/resources/clicks.txt'," +
                " 'format'='csv' " +
                ")";
        tableEnv.executeSql(clickDDL);
        tableEnv.sqlQuery("select count(1) from clickTable ").execute().print();

        // 创建表函数并注册
        tableEnv.createTemporarySystemFunction("mytableFunction", MytableFunction.class);


        // 查询
        String sqlQuery = "select user_name,url,split_result  from clickTable ,lateral table ( mytableFunction(url) ) as T(split_result) ";
        Table tableResult = tableEnv.sqlQuery(sqlQuery);
        tableEnv.toDataStream(tableResult).print("tableFunction");


        env.execute();
    }

    public static class MytableFunction extends TableFunction<String> {
        public void eval(String url) {
            String[] fields = url.split("\\&");
            for (String field : fields) {
                collect(field);
            }

        }
    }
}
