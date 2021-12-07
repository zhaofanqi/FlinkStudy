package demo3Param;

import kafka.server.BrokerConfigHandler;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringSerializer;
import test.StringTest;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * 用于解释说明常用 producer 参数含义
 * 发送批次控制： ProducerConfig.BATCH_SIZE_CONFIG    和 ProducerConfig.LINGER_MS_CONFIG
 * 前者以信息为单位，当信息大小累加和>=batch-size时，发送消息
 * 后者信息延迟发送，信息产生到发送所能等待的时间
 * 二者共同作用了批量发送。此处需要注意 BATCH_SIZE_CONFIG的设定！ 当需要指定条数发送时，需要计算record的元信息大小，Batch-header大小
 * 消息发送，分区选择策略：send进行追踪
 * 1 当record中指定分区时则发往指定分区
 * 2 按照record中key的hash值取模发往指定分区
 * 3 当1,2不满足时，若可用分区数量=1则直接发送，若不为 1 则随机发送
 */
public class MyProducer {
    public static void main(String[] args) {
        Properties properties = new Properties();
        //基础配置
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "tbds-172-16-122-50:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        /**
         * ProducerConfig.ACKS_CONFIG 默认是 -1 ，可选值为 ："all", "-1", "0", "1"
         *                      -1  和 all 表示生产者需要收到 master的所有副本的ack
         *                      1 表示生产者需要收到 master 的 ack ，认为发送成功
         *                      0 表示生产者无需收到 master 的 ack，就认为发送成功
         */
        properties.put(ProducerConfig.ACKS_CONFIG, "-1");

        /**
         *  消息发送重试次数 默认值 Integer.MAX_VALUE
         */
        properties.put(ProducerConfig.RETRIES_CONFIG,"3");


        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,"demo3Param.MyPartition");

        /**
         * ProducerConfig.CLIENT_ID_CONFIG 用于说明是哪个客户端生产的数据，默认为 “”
         */
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "zhaofq_computer");

        /**
         * ProducerConfig.BATCH_SIZE_CONFIG 发送消息的大小，单位是 16384 bytes(1024*16) 太大浪费内存，太小不断在client 与 server端请求
         *      ProducerRecord元数据 7 字节
         *      Batch也有自身的元数据，这些元数据大小 61个字节  所以 1024*5 只能发送4条信息
         *      61+7*5=96
         *      128可以 100不行
         *      5条  1024*5+110  5*x+y=110
         *      6条  1024*6+110  6*x+y=120   所以一条消息的元数据大小为 10个byte 一个batch的元数据大小为60
         *      验证： 发四条则 1024*4+100  1024*4+99则只能发3条
         */
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 1024 * 4 + 100);
        /**
         *  ProducerConfig.MAX_REQUEST_SIZE_CONFIG 生产者发送请求的最大值
         */
        properties.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 1024 * 1024);
        /**
         * ProducerConfig.BUFFER_MEMORY_CONFIG 默认值 32 * 1024 * 1024L，即 32MB 缓存发往 server 的record，
         *              producer产生的消息会先写入 buffer_memory中，然后通过sender线程发送，当producer生成的速度太大，
         *                      占满 buffer_memory时，会阻塞 producer 。注意 sender是以batch来发送消息
         *                      如果发送消息速度大于broker接受速度，超过MAX_BLOCK_MS_CONFIG时间则会报错
         */
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 32 * 1024 * 1024L);
        /**
         * ProducerConfig.LINGER_MS_CONFIG 默认值为0 ，单位为ms，表示有消息就发送
         *                                  设置一个值后，会延迟发送
         */
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 10 * 1000);
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        String line = "";
        line = StringTest.specifyLengthString(5, 'c');
        // 构建发送消息大小，consumer测试消费次数
        int count = 0;
        ProducerRecord<String, String> record;
        /*for (String configName : ProducerConfig.configNames()) {
            System.out.println(configName);
        }*/
        while (true) {
            try {
                record = new ProducerRecord<>("zhaofq_test1019", line);
                producer.send(record);
             /*   RecordMetadata zfq = producer.send(record).get();
                System.out.println(zfq.timestamp());*/
                System.out.println("发送次数" + (count++));
//                Thread.sleep(1000 * 1);
            } catch (Exception e) {
                e.printStackTrace();
            }
            if (count > 5) {
                break;
            }
        }
        producer.close();
    }

}
