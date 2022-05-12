package cep;

import entity.LoginEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * ClassName StartDemo
 *
 * @Auther: 赵繁旗
 * @Date: 2022/5/11 08:34
 * @Description: CEP 使用步骤：
 * 注册规则
 * 将规则应用与流进行匹配
 * 对匹配结果进行处理
 *      量词： oneOrMore
 *            times 默认是宽松近邻，如果想更改为严格近邻使用 .times().consecutive
 *                               如果想更改为非确定性宽松近邻 .times().allowCombinations
 *            greedy
 *            optional
 *
 *            连接词 next        :严格近邻 中间不允许有其他插入
 *                  followBy    :宽松近邻（允许中间有其他插入）
 *                  followByAny :非确定性宽松近邻，可以重复使用之前匹配到的时间
 *                  notNext
 *                  notFollowBy :一般用于2个事件之间没有某个事件如： a.notFollowBy(b).followBy(c) 即：a和c之间没有b
 *
 *            条件    where or 可以在单一事件里面写入多个判断逻辑也可以写多个where or进行逻辑判断
 *                   within() 时间限定匹配有效。模式中只会时间最多的那个生效
 * <p>
 * 案例连续登陆三次用户失败用户
 */
public class StartDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        SingleOutputStreamOperator<LoginEvent> loginSoure = env.readTextFile("flink/src/main/resources/loginRecord.txt").map(new MapFunction<String, LoginEvent>() {
            @Override
            public LoginEvent map(String value) throws Exception {
                String[] fields = value.split(",");
                return new LoginEvent(fields[0], fields[1], fields[2], Long.valueOf(fields[3]));
            }
        });
        SingleOutputStreamOperator<LoginEvent> loginWM = loginSoure.assignTimestampsAndWatermarks(WatermarkStrategy
                .<LoginEvent>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                .withTimestampAssigner(new SerializableTimestampAssigner<LoginEvent>() {
                    @Override
                    public long extractTimestamp(LoginEvent element, long recordTimestamp) {
                        return element.loginTimeStamp;
                    }
                }));
//        loginWM.print("test");

        //1 定义规则
        Pattern<LoginEvent, LoginEvent> loginFailePattern = Pattern.<LoginEvent>begin("first")
                .where(new IterativeCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value, Context<LoginEvent> ctx) throws Exception {
                        return value.loginStatus.equals("fail");
                    }
                }).next("second")
                .where(new IterativeCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value, Context<LoginEvent> ctx) throws Exception {
                        return value.loginStatus.equals("fail");
                    }
                }).next("third")
                .where(new IterativeCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value, Context<LoginEvent> ctx) throws Exception {
                        return value.loginStatus.equals("fail");
                    }
                });
        // 流上规则匹配
        PatternStream<LoginEvent> pattern = CEP.pattern(loginWM.keyBy(loginEvent->loginEvent.userName), loginFailePattern);

        // 匹配流进行处理
        SingleOutputStreamOperator<String> patternResult = pattern.select(new PatternSelectFunction<LoginEvent, String>() {
            @Override
            public String select(Map<String, List<LoginEvent>> pattern) throws Exception {
                LoginEvent firstEvnent = pattern.get("first").get(0);
                LoginEvent secondEvnent = pattern.get("second").get(0);
                LoginEvent thirdEvnent = pattern.get("third").get(0);
                return firstEvnent.userName+"不正确的时间分别为"+"\t"+firstEvnent.loginTimeStamp+"\t"+secondEvnent.loginTimeStamp+"\t"+thirdEvnent.loginTimeStamp;
            }
        });
        patternResult.print("pattern");

        env.execute();


    }

}
