package OperateStream;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * ClassName ConnectStream
 *
 * @Auther: 赵繁旗
 * @Date: 2022/4/27 08:51
 * @Description:    流的合并：流的类型可以不同，但是合并流处理后，返回值类型要相同
 */
public class ConnectStream {
    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Integer> source1 = env.fromElements(1, 2, 3, 4);
        DataStreamSource<Long> source2 = env.fromElements(5L, 6L, 7L, 8L);

        ConnectedStreams<Integer, Long> connectedStreams = source1.connect(source2);

        SingleOutputStreamOperator<String> mapResult = connectedStreams.map(new CoMapFunction<Integer, Long, String>() {
            @Override
            public String map1(Integer value) throws Exception {
                return "integer:\t" + value;
            }

            @Override
            public String map2(Long value) throws Exception {
                return "long:\t" + value;
            }
        });
        mapResult.print("final");


        env.execute();


    }
}
