package sink;

import entity.Click;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.util.concurrent.TimeUnit;

/**
 * ClassName FileSink
 *
 * @Auther: 赵繁旗
 * @Date: 2022/4/20 14:05
 * @Description: flink 可以将数据写出到多种存储路径下
 * 此处尝试写入本地文件系统
 */
public class FileSink {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> clicks = env.readTextFile("./flink/src/main/resources/clicks.txt");

        SingleOutputStreamOperator<Click> clickStream = clicks.map(x -> {
            String[] fields = x.split(",");
            return new Click(fields[0], fields[1], Long.parseLong(fields[2]));
        });

        StreamingFileSink<String> fileSink = StreamingFileSink.<String>forRowFormat(
                            new Path("./output"), new SimpleStringEncoder<>("utf-8"))
                                            .withRollingPolicy(
                                                    DefaultRollingPolicy.builder()
                                                    .withMaxPartSize(1024*1024*1)// 1M则新建文件
                                                    .withInactivityInterval(TimeUnit.MINUTES.toMillis(15))  // 写入时间达到15分钟时新建文件
                                                    .withRolloverInterval(TimeUnit.MINUTES.toMillis(5))// 5 分钟没有写入则再次写入时新建文件
                                                    .build())// withRollingPolicy 写入数据滚动策略
                                            .withOutputFileConfig(new OutputFileConfig("zhaofq","tao"))
                                            .build();
        clickStream.
                map(x -> x.toString())
                .addSink(fileSink)
                .setParallelism(2);

        env.execute();


    }
}
