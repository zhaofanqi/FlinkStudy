package UDFunction;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.FunctionContext;
import scala.Tuple2;

/**
 * ClassName AggregateFunctionTest
 *
 * @Auther: 赵繁旗
 * @Date: 2022/5/10 14:35
 * @Description: 聚合函数 中 createAccumulate() accumulate()
 * <p>
 * 实现加权平均值的计算  时间*次数+时间*次数
 */
public class AggregateFunctionTest {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 创建 临时表并测试
        String clickDDL = "create table clickTable (" +
                "user_name String," +
                "url String," +
                "ts bigint," +
                "et as to_timeStamp(from_unixtime(ts/1000))," +
                "WATERMARK FOR et as et - interval '1' second " +
                ") with (" +
                "'connector'='filesystem'," +
                "'path'='flink/src/main/resources/clicks.txt'," +
                "'format'='csv'" +
                ")";
        tableEnv.executeSql(clickDDL);
//        tableEnv.sqlQuery("select count(1) from clickTable ").execute().print();
        // 编写自定义实现聚合函数 并注册
        tableEnv.createTemporarySystemFunction("aggregateFunction", UDFAggregateFunction.class);

        // 使用聚合函数查询
        String query="select user_name,url,aggregateFunction(ts,1) as avg_result from clickTable  group by user_name,url";
        Table querytable = tableEnv.sqlQuery(query);
        tableEnv.toChangelogStream(querytable).print("query");


        env.execute();
    }

    public static class Aggre {
        public long sum=0;
        public int count=0;
    }

    public static class UDFAggregateFunction extends AggregateFunction<Long, Aggre> {

        @Override
        public Long getValue(Aggre accumulator) {
            if (accumulator.sum==0|| accumulator.count==0){
                return null;
            }
            return accumulator.sum / accumulator.count;
        }

        public void accumulate(Aggre accumulator, Long ts, int cnt) {
            accumulator.sum += ts * cnt;
            accumulator.count += cnt;
        }

        @Override
        public Aggre createAccumulator() {
            return new Aggre();
        }
    }
}
