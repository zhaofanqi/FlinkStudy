package demo.CheatCheck;


import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FraudDetectionJob {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //添加数据源
        DataStreamSource<Trancation> dsSource = env.addSource(new TrancationSource());
        //添加输出
        dsSource.print();
        dsSource.keyBy(Trancation::getId).process(new FraudDetector()).name("alert-segment");
        //任务执行
        env.execute("first flink test");
    }
}
