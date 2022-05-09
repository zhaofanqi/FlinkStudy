package tableAPI;

import entity.Click;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import utils.CreateClicks;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

/**
 * ClassName TableWithWM
 *
 * @Auther: 赵繁旗
 * @Date: 2022/5/7 16:03
 * @Description:    表中指定水位线
 *                      由于水位线涉及时间语义，因为分处理时间 事件时间
 *                      水位线指定方式： 创建DDL时指定
 *                                     流转换时指定
 *                             处理时间使用 proctime (直接增加一个时间字段即可)
 *                             事件时间使用 rowtime (将流中时间字段转换)
 */
public class TableWithWM {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 1 处理时间
        // 1.1 流中指定水位线
        DataStreamSource<Click> clickSource = env.addSource(new CreateClicks());

        Table proceTable1 = tableEnv.fromDataStream(clickSource, $("user"), $("url").as("aa"), $("`timeStamp`").proctime());
        proceTable1.printSchema();

        // 1.2 DDL中指定水位线
        String proDDL="create table proDDL (" +
                "user_name String," +
                "url String," +
                "ts BIGINT," +
                "ts_wm  AS PROCTIME()" +
                ") with (" +
                "'connector'='filesystem'," +
                "'path'='flink/src/main/resources/clicks.txt'," +
                "'format'='csv'" +
                ")";
        tableEnv.executeSql(proDDL);
        Table proTable = tableEnv.sqlQuery("select * from proDDL ");
        proTable.printSchema();
        System.out.println("======");

        // 2 事件时间
        // 2.1 流中获取

        SingleOutputStreamOperator<Click> clickWM = clickSource.assignTimestampsAndWatermarks(WatermarkStrategy
                .<Click>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<Click>() {
                    @Override
                    public long extractTimestamp(Click element, long recordTimestamp) {
                        return element.timeStamp;
                    }
                }));
        Table eventTable = tableEnv.fromDataStream(clickWM, $("user"), $("url").as("aa"), $("`timeStamp`").rowtime());
        eventTable.printSchema();

        // 2.2 DDL中指定水位线
        String eventDDL="create table eventDDL(" +
                "user_name String," +
                "url String," +
                "ts BIGINT," +
                "et AS to_timestamp(from_unixtime(ts/1000))," +
                "WATERMARK FOR et AS et - INTERVAL '5' SECOND " +
                ") with (" +
                "'connector' = 'filesystem'," +
                "'path' = 'flink/src/main/resources/clicks.txt'," +
                "'format' = 'csv'" +
                ")";
        tableEnv.executeSql(eventDDL);

        Table aggResult = tableEnv.sqlQuery("select count(1) from eventDDL");
        aggResult.execute().print();
        env.execute();

    }
}
