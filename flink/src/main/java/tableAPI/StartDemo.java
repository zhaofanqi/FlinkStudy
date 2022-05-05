package tableAPI;

import entity.Click;
import entity.Users;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import utils.CreateClicks;
import utils.CreateUsers;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

/**
 * ClassName StartDemo
 *
 * @Auther: 赵繁旗
 * @Date: 2022/5/5 09:49
 * @Description:
 */
public class StartDemo {
    public static void main(String[] args) throws Exception {
        // 1 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 2 创建 sql执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 3 获取输入源
        DataStreamSource<Click> clickSource = env.addSource(new CreateClicks());
        SingleOutputStreamOperator<Click> clickSourceWM = clickSource.assignTimestampsAndWatermarks(WatermarkStrategy
                .<Click>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<Click>() {
                    @Override
                    public long extractTimestamp(Click element, long recordTimestamp) {
                        return element.timeStamp;
                    }
                }));
        // 获取输入源2
        DataStreamSource<Users> usersSource = env.fromElements(
                new Users(1,"zhaofq","male"),
                new Users(2,"taoxp","female")
        );


        clickSourceWM.print("source");
        Table userTable = tableEnv.fromDataStream(usersSource);
        // 4 将数据转为表
        Table clickTable = tableEnv.fromDataStream(clickSourceWM);
        // 5.1 对表数据进行查询
        Table select1 = clickTable.select($("user"), $("url"));
        tableEnv.toDataStream(select1).print("select_1");
        // 5.2 对表数据进行查询
        Table clickTable2 = tableEnv.sqlQuery("select * from " + clickTable);
        tableEnv.toDataStream(clickTable2).print("select_2");

        // 将表中数据输出（需要转DataStream）；直接输出表为表结构
        // 6 任务执行
        env.execute();


    }
}
