package tableAPI;


import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * ClassName CreateEnv3Method
 *
 * @Auther: 赵繁旗
 * @Date: 2022/5/5 11:29
 * @Description: 可以三种方式直接创建 table API 环境
 */
public class CreateEnv3Method {
    public static void main(String[] args) throws Exception {
        // 方式一：基于流的方式创建执行环境
      /*  StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv1 = StreamTableEnvironment.create(env);
*/
        // 方式二： 直接使用TableEnvironment创建  inStreamingMode useBlinkPlanner是默认的
        EnvironmentSettings envSettings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .useBlinkPlanner()
                .build();
        TableEnvironment tableEnv2 = TableEnvironment.create(envSettings);
        // 方式三： 老版本解释器的方式
       /* ExecutionEnvironment oldEnv = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment oldTableEnv = BatchTableEnvironment.create(oldEnv);
*/
        // 创建输入表方式：connector 方式； 虚拟表
        String createDDL = "create table click (" +
                "user_name String," +
                "url String ," +
                "time_stamp BIGINT" +
                ") with ( " +
                "'connector'='filesystem'," +
                "'path'='flink/src/main/resources/clicks.txt'," +
                "'format'='csv'" +
                ")";
        // 完成connector 表的创建
        tableEnv2.executeSql(createDDL);
         // 虚拟表的创建 表对象 + createTemporaryView
        //    表对象获取
        Table tableQuery = tableEnv2.sqlQuery("select user_name,time_stamp from click  ");
//      控制台输出查看结果
//         tableQuery.execute().print();
        //    createTemporaryView
        tableEnv2.createTemporaryView("tableQuery", tableQuery.where($("user_name").isEqual("lisi")).select($("user_name")));

//        Table tableQuery_result = tableEnv2.sqlQuery("select user_name,count(user_name) as cn   from tableQuery  group by user_name");
        Table tableQuery_result = tableEnv2.sqlQuery("select user_name, 1  from click ");
        //  输出表的创建
        String outputDDL="create table output(" +
                "user_name String," +
                " cn BIGINT" +
                ") with (" +
                "'connector' = 'filesystem'," +
                "'path' = 'flink/src/main/resources/output_table'," +
                "'format'='csv'" +
                ")";
        tableEnv2.executeSql(outputDDL);
        tableQuery_result.executeInsert("output");

        // 将结果输出到控制台 create table printTable
        // Noted: 报错可能是sql查询插入连接表时类型不匹配，并非建表语句有毛病
        String printDDL="create table printTable(" +
                "user_name String," +
                " cn BIGINT" +
                ") with (" +
                "'connector' = 'print'" +
                ")";

        tableEnv2.executeSql(printDDL);
        Table aggResult = tableEnv2.sqlQuery("select user_name ,count(time_stamp) as cn from click group by user_name");
        aggResult.executeInsert("printTable");


    }
}
