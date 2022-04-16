package source.kafka;


import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;


/**
 * flink从 kafka读取数据并直接打印
 * TODO 因为公司环境使用遇到问题，暂时跳过接入kafka数据源
 */
public class FlinkConsumerKafka {
    public static void main(String[] args) throws Exception {
        // 1 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2 添加数据源头 由于是 Kafka flink常见，可去找包

        Properties prop=new Properties();
        prop.put(ConsumerConfig.GROUP_ID_CONFIG, "kafkaParam2");
        prop.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "tbds-172-16-122-50:9092");
        prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class);

        FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer<String>("zhaofq", new SimpleStringSchema(), prop);
        DataStreamSource<String> flinkSource = env.addSource(kafkaSource);

        // 3 转换操作
        flinkSource.print();
        // 4 数据输出
        // 5 出发程序
        env.execute();

    }
}
