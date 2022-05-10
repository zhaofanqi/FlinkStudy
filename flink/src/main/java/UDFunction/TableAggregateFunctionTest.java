package UDFunction;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.util.Collector;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * ClassName TableAggregateFunctionTest
 *
 * @Auther: 赵繁旗
 * @Date: 2022/5/10 14:51
 * @Description:  取每个窗口的top2
 *          表聚合函数 ：先聚合再做表处理。过程为 1-->n 和 1--->n
 *
 */
public class TableAggregateFunctionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 创建表
        String clickDDL="create table clickTable(" +
                "user_name  String," +
                "url String ," +
                "ts bigint," +
                "et as to_timeStamp(from_unixTime(ts/1000))," +
                "WATERMARK FOR et as et - interval '1' second " +
                ") with (" +
                "'connector'='filesystem'," +
                "'path'='flink/src/main/resources/clicks.txt'," +
                "'format'='csv'" +
                ")";
        tableEnv.executeSql(clickDDL);
//        tableEnv.toChangelogStream(tableEnv.sqlQuery("select count(1) from clickTable")).print("test");

        // 注册表聚合函数
        tableEnv.createTemporarySystemFunction("tableAggreFunction",TableAggreFunction.class);
        // 读取数据
        //  先使用子查询得到每个窗口的查询次数
        Table cntTable = tableEnv.sqlQuery("select user_name,window_start,window_end, count(1) as cnt " +
                " from table (tumble(Table clickTable,descriptor(et),interval '10' second))" +
                " group by user_name,window_start,window_end");
//        tableEnv.toChangelogStream(cntTable).print("cntTable");

        // NOTED: 由于 对SQL 对表聚合函数支持不好，建议使用 table API
        // 因为传入参数只有一个cnt，因为输出时只有聚合结果 和聚合字段 因而进行调整输出参数格式
        Table finalResult = cntTable.groupBy($("window_end"))
                .flatAggregate(call("tableAggreFunction", $("cnt"),$("user_name")).as("user_name","value","rank"))
                .select($("window_end"),$("user_name"), $("value"), $("rank"));

        tableEnv.toChangelogStream(finalResult).print("final");

        env.execute();
    }

    public static class Top2{
        public long maxValue;
        public long secondValue;
        public String user_name;
    }
    public static class TableAggreFunction  extends TableAggregateFunction<Tuple3<String,Long,Integer>,Top2>{
        @Override
        public Top2 createAccumulator() {
            Top2 top2 = new Top2();
            top2.maxValue=Long.MIN_VALUE;
            top2.secondValue=Long.MIN_VALUE;
            return top2;
        }

        public void accumulate(Top2 top2,long cnt,String user_name ){
                if (cnt>top2.maxValue){
                    top2.secondValue=top2.maxValue;
                    top2.maxValue=cnt;
                    top2.user_name=user_name;
                }else  if(cnt>top2.secondValue){
                    top2.secondValue=cnt;
                    top2.user_name=user_name;
                }
        }

        public void emitValue(Top2 top2, Collector<Tuple3<String,Long,Integer>> out){

            if (top2.maxValue!=Long.MIN_VALUE){
                out.collect(Tuple3.of(top2.user_name,top2.maxValue,1));
            }
            if (top2.secondValue!=Long.MIN_VALUE){
                out.collect(Tuple3.of(top2.user_name,top2.secondValue,2));
            }
        }


    }
}
