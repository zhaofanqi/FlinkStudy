package state;

import entity.Click;
import entity.UrlCountEnd;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.BroadcastStream;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * ClassName BroadcastState
 *
 * @Auther: 赵繁旗
 * @Date: 2022/5/2 09:33
 * @Description: 使用广播流广播状态匹配
 * stream.broadcast()
 */
public class BroadcastState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Click> clickSource = env.fromElements(
                new Click("zhaofq", "/product", System.currentTimeMillis()),
                new Click("zhaofq", "/home", System.currentTimeMillis()),
                new Click("zhaofq", "/carts", System.currentTimeMillis())
        );
        //UrlCountEnd(String url, Integer count, Long windowEnd)
        DataStreamSource<UrlCountEnd> countSource = env.fromElements(
                new UrlCountEnd("/home", 3, System.currentTimeMillis()),
                new UrlCountEnd("/product", 4, System.currentTimeMillis()),
                new UrlCountEnd("/carts", 5, System.currentTimeMillis()),
                new UrlCountEnd("/food", 6, System.currentTimeMillis())
        );

        //将统计流广播,匹配正在发生点击的数据
        // 定义广播状态描述器
        MapStateDescriptor<Void, UrlCountEnd> broadDesc = new MapStateDescriptor<>("broad", Types.VOID, Types.POJO(UrlCountEnd.class));
        BroadcastStream<UrlCountEnd> broadcastStream = countSource.broadcast(broadDesc);

        SingleOutputStreamOperator<Tuple2<String, UrlCountEnd>> processResult = clickSource.keyBy(data -> data.url)
                .connect(broadcastStream)
                .process(new KeyedBroadcastProcessFunction<String, Click, UrlCountEnd, Tuple2<String, UrlCountEnd>>() {
                    @Override
                    public void processElement(Click value, ReadOnlyContext ctx, Collector<Tuple2<String, UrlCountEnd>> out) throws Exception {

                        System.out.println("processElement====" + value);
                        // 从广播状态中获取状态
                        ReadOnlyBroadcastState<Void, UrlCountEnd> brod = ctx.getBroadcastState(new MapStateDescriptor<>("broad", Types.VOID, Types.POJO(UrlCountEnd.class)));

                        UrlCountEnd urlCountEnd = brod.get(null);

                        if (urlCountEnd != null) {
                            if (urlCountEnd.url.equals(value.url)) {
                                out.collect(Tuple2.of(ctx.getCurrentKey(), urlCountEnd));
                            }
                        }

                    }

                    @Override
                    public void processBroadcastElement(UrlCountEnd value, Context ctx, Collector<Tuple2<String, UrlCountEnd>> out) throws Exception {

                        // 从上下文中获取广播状态，并用当前数据更新状态  new MapStateDescriptor<>("brod", Types.VOID, Types.POJO(UrlCountEnd.class))
                        org.apache.flink.api.common.state.BroadcastState<Void, UrlCountEnd> broadcastState = ctx.getBroadcastState(new MapStateDescriptor<>("broad", Types.VOID, Types.POJO(UrlCountEnd.class)));
                        broadcastState.put(null, value);
                        System.out.println("processBroadcastElement====" + value);
                    }
                });
        processResult.print();
        env.execute();
    }
}
