package source.kafka;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * ClassName FlinkConsumerKafkaOffical
 *
 * @Auther: 赵繁旗
 * @Date: 2022/4/14 11:07
 * @Description:
 */
public class FlinkConsumerKafkaOffical {
    public static void main(String[] args) throws Exception {
        //TODO 测试
        // 1 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2 添加数据源头 由于是 Kafka flink常见，可去找包
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("")
                .setTopics("flink_test")
                .setGroupId("zhaofanqi")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> kafka_source = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        // 3 转换操作
        kafka_source.print();
        // 4 数据输出
        // 5 触发程序
        env.execute();
    }
}
