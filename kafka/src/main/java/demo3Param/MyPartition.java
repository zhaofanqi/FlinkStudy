package demo3Param;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

public class MyPartition implements Partitioner {
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        /*System.out.println( topic+"====\t===");
        System.out.println( value+"====\t===");
        String strBytesValue = new String(valueBytes);
        System.out.println( strBytesValue+"====\t===");*/
        return 1;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
