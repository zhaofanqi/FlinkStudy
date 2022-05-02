package state;

import entity.Click;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.checkpoint.Checkpoint;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import utils.CreateClicks;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * ClassName OperatorListState
 *
 * @Auther: 赵繁旗
 * @Date: 2022/5/1 15:36
 * @Description: 通过Operate 的 ListState 看 checkpoint
 */
public class OperatorListState {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Click> clickSource = env.addSource(new CreateClicks());
        SingleOutputStreamOperator<Click> clickWM = clickSource.assignTimestampsAndWatermarks(WatermarkStrategy
                .<Click>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<Click>() {
                    @Override
                    public long extractTimestamp(Click element, long recordTimestamp) {
                        return element.timeStamp;
                    }
                }));

        clickWM.print();
        clickWM.addSink(new MySink(5));


        env.execute();


    }

    // SinkFunction 输出
    // CheckpointedFunction 检查点
    private static class MySink implements SinkFunction<Click>, CheckpointedFunction {

        private List<Click> list;
        private int threshold;


        private ListState<Click> globalList;

        public MySink(int threshold) {
            this.list = new ArrayList<Click>();
            this.threshold = threshold;

        }


        // 状态初始化，状态数据的恢复
        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            globalList = context.getOperatorStateStore().getListState(new ListStateDescriptor<Click>("click", Click.class));
            // 如果是恢复的话，则将记录恢复到 list中
            if (context.isRestored()) {
                for (Click click : globalList.get()) {
                    list.add(click);
                }
            }
        }

        @Override
        public void invoke(Click value, Context context) throws Exception {
            list.add(value);
            System.out.println("****" + list.size());
            if (list.size() == threshold) {
                for (Click click : list) {
                    System.out.println(click);
                }
                System.out.println("=================");
                list.clear();
            }

        }


        // 快照存储数据，每 5 条数据就快照一次
        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            globalList.clear();
            for (Click click : list) {
                globalList.add(click);
            }
        }
    }
}
