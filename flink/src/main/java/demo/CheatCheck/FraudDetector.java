package demo.CheatCheck;


import jdk.nashorn.internal.codegen.types.Type;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.io.IOException;

public class FraudDetector extends KeyedProcessFunction<Integer, Trancation, Alert> {
    private static final long serialVersionUID = 1L;
    private static final double SMALL_AMOUNT = 1.00;
    private static final double LARGE_AMOUNT = 500.00;
    private static final long ONE_MINUTE = 60 * 1000;

    private transient ValueState<Boolean> flagState;
    private transient ValueState<Long> timeState;

    @Override
    public void processElement(Trancation trancation, Context context, Collector<Alert> collector) throws Exception {
        // Get the current state for the current key
        Boolean lastTransactionWasSmall = flagState.value();
        // Check if the flag is set
        if (lastTransactionWasSmall != null) {
            if (trancation.getAmount() > LARGE_AMOUNT) {
                Alert alert = new Alert();
                alert.setAid(trancation.getId());
                collector.collect(alert);
            }
            // Clean up our state
            flagState.clear();
        }
        if (trancation.getAmount() < SMALL_AMOUNT) {
            // Set the flag to true
            flagState.update(true);
            long timer = context.timerService().currentProcessingTime() + ONE_MINUTE;
            context.timerService().registerEventTimeTimer(timer);
            timeState.update(timer);
        }

    }

    // 流处理增加状态管理
    // 状态的管理使用open
    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<Boolean> flag = new ValueStateDescriptor<>("flag", Types.BOOLEAN);
        flagState = getRuntimeContext().getState(flag);

        ValueStateDescriptor<Long> timeDescriptor = new ValueStateDescriptor<>("time-state", Types.LONG);
        timeState = getRuntimeContext().getState(timeDescriptor);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Alert> out) throws Exception {
        flagState.clear();
        timeState.clear();
    }

    /**
     *   取消定时器，你需要删除已经注册的定时器，并同时清空保存定时器的状态
     * @param ctx
     * @throws IOException
     */
    private void cleanup(Context ctx) throws IOException {
        Long time = timeState.value();
        ctx.timerService().deleteProcessingTimeTimer(time);
        this.timeState.clear();
        flagState.clear();

    }
}
