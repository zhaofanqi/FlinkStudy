package tableAPI;

import org.apache.flink.api.scala.typeutils.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * ClassName TopNDemo
 *
 * @Auther: 赵繁旗
 * @Date: 2022/5/10 08:02
 * @Description: 查询topN
 * 第一种：直接基于历史数据
 * 第二种：基于最近一段时间数据统计
 */
public class TopNDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 创建临时表
        String clickTableDDL = "create table clickTable (" +
                "user_name String," +
                "url String," +
                "ts BIGINT," +
                "et AS to_timestamp(from_unixtime(ts/1000))," +
                "WATERMARK FOR et AS  et - interval '1' second" +
                ") with (" +
                "'connector'='filesystem'," +
                "'path'='flink/src/main/resources/clicks.txt'," +
                "'format'='csv'" +
                ")";
        tableEnv.executeSql(clickTableDDL);
        //测试表是否成功生成
        Table testTable = tableEnv.sqlQuery("select user_name,count(url) as cnt from clickTable group by user_name");
//        tableEnv.toChangelogStream(testTable).print();


//        String top="select user_name,row_number() over(partition by user_name order by cnt desc ) as row_num from " +
        String top = " select user_name ,row_num from ( " +
                "        select user_name,cnt,ROW_NUMBER() OVER(partition by user_name order by cnt desc ) as row_num from " +
                "           ( " +
                "           select user_name,count(url) as cnt from clickTable group by user_name " +
                "           )   a " +
                "           ) b where row_num<=2 ";
//        System.out.println(top);
        //NOTED: 运行失败，不一定是编写sql有问题而是工具缓存问题，可以试着重启
        Table topResult = tableEnv.sqlQuery(top);
        topResult.printSchema();
//        tableEnv.toChangelogStream(topResult).print("top");

        // 开窗topN

        String windowCount = "select user_name,count(1) as cnt ,window_start,window_end from  " +
                " table (" +
                "tumble(table clickTable ,descriptor(et),interval '10' second )" +
                ")  group by user_name,window_start,window_end  " ;
        tableEnv.sqlQuery(windowCount).execute().print();
        System.out.println("=======");

        String topNWindowSQL="select user_name,window_start,window_end ,row_num from (" +
                "select user_name,window_start,window_end,row_number() over(partition by window_start,window_end order by cnt desc) as row_num from ( " +
                windowCount+" ) a" +
                " ) b where b.row_num<=2";
        System.out.println(topNWindowSQL);
        Table topNWindow = tableEnv.sqlQuery(topNWindowSQL);
        tableEnv.toChangelogStream(topNWindow).print("window");


        // 测试 row_number() over 以后没有截取topN 是否会报错,经测试的确是个错误
      /*  String topTest="" +
                "select user_name,window_start,window_end,row_number() over(partition by window_start,window_end order by cnt desc) as row_num from ( " +
                windowCount+" ) a"
               ;
        System.out.println(topTest);
        Table topTestTable = tableEnv.sqlQuery(topTest);
        tableEnv.toChangelogStream(topTestTable).print("topTest");*/


        env.execute();

    }
}
