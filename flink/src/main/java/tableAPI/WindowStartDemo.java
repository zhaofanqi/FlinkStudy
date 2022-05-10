package tableAPI;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * ClassName WindowStartDemo
 *
 * @Auther: 赵繁旗
 * @Date: 2022/5/9 11:22
 * @Description: 表中开窗口的使用：
 * 窗口类型：    tumble    滚动
 *              hop       滑动
 *              session   会话
 *              cumulate      累计窗口 有窗口大小，有窗口步长：在窗口大小内不断累计，每达到一次步长输出一次汇总结果
 *
 *         开窗函数： count() over(partition by column oder by time_column  范围 )
 *                          范围：只能到当前行   如：   range between interval '5' minute preceding and current row
 *                                                   row between 5 preceding and current row
 */
public class WindowStartDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //创建表
        String clickDDL = "create table clickTable (" +
                "user_name String," +
                "url STRING," +
                "ts BIGINT," +
                "et AS to_timestamp(from_unixtime(ts/1000))," +
                "WATERMARK FOR et  AS et - INTERVAL '1' SECOND" +
                ") with (" +
                "'connector'='filesystem'," +
                "'path'='flink/src/main/resources/clicks.txt'," +
                "'format'='csv' " +
                ")";
        tableEnv.executeSql(clickDDL);
        // 1 分组聚合
        Table aggResult = tableEnv.sqlQuery("select user_name,count(1) as cnt from clickTable group by  user_name ");
//        tableEnv.toChangelogStream(aggResult).print();


        // 2 分组窗口聚合-- 老版本
        // NOTED 要特别注意编写的格式
        Table windowQueray = tableEnv.sqlQuery(
                " select  user_name , count(1) as cnt ,TUMBLE_END(et,INTERVAL '5' SECOND) AS ent_T " +
                        " from clickTable " +
                        " group by " +
                        " user_name," +
                        " TUMBLE(et,INTERVAL '5' SECOND)");
//        tableEnv.toChangelogStream(windowQueray).print("windowResult");

        // 3分组窗口聚合-- 滚动
        Table tumbleWindowQuery = tableEnv.sqlQuery("select user_name,count(1) as cnt, window_end as ent_T " +
                " from table( " +
                " tumble(table clickTable,descriptor(et),interval '5' second)" +
                ")  " +
                " group by user_name,window_start,window_end " +
                " ");
//        tableEnv.toDataStream(tumbleWindowQuery).print("tumbleWindowQuery");

        // 3分组窗口聚合-- 滑动
        Table hopWindowQuery = tableEnv.sqlQuery("select user_name,count(1) as cnt ,window_end as w_end " +
                " from table (" +
                " hop(table clickTable,descriptor(et),interval '5' second,interval '10' second )" +
                " )" +
                " group by user_name,window_start,window_end");
//        tableEnv.toDataStream(hopWindowQuery).print("hop");

        // 3分组窗口聚合 -- 累积
        Table cumulateWindowQuery = tableEnv.sqlQuery("select user_name,count(1) as cnt ,window_end as w_end " +
                " from table (" +
                " cumulate(table clickTable,descriptor(et),interval '5' second,interval '10' second)" +
                ")" +
                " group by user_name,window_start,window_end");
//        tableEnv.toDataStream(cumulateWindowQuery).print("cumulate");


        //4 开窗函数 -- 还可以将表中的乱序数据排好序
        // rows between
        Table rowOver = tableEnv.sqlQuery(" select user_name,ts," +
                " avg(ts) over (partition by user_name order by et rows between 3 preceding and current row) " +
                " from clickTable ");
//        tableEnv.toDataStream(rowOver).print("rows");

        // range between interval '5' second preceding
        Table rangeOver = tableEnv.sqlQuery(" select user_name, ts ," +
                "avg(ts) over(partition by user_name order by et range between interval '3' second preceding and current row ) as avg_ts " +
                " from clickTable ");
//        tableEnv.toDataStream(rangeOver).print("rows");

        // 多个相同窗口简写

        Table simpleMultiWindow = tableEnv.sqlQuery("select user_name,ts,url," +
                " avg(ts) over w as avg_ts ," +
                " sum(ts) over w  as sum_ts  " +
                " from clickTable " +
                " window w as (partition by user_name order by et rows between 3 preceding and current row) ");
        tableEnv.toDataStream(simpleMultiWindow).print("simple");



        env.execute();
    }
}
