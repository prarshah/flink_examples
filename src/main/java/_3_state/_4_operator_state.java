package _3_state;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.ArrayList;
import java.util.List;

public class _4_operator_state implements SinkFunction<Tuple2<Integer,Integer>>, CheckpointedFunction {

private final int threashold ;
private transient ListState<Tuple2<Integer,Integer>> checkpointState;
private List<Tuple2<Integer,Integer>> bufferedElements;

public _4_operator_state(int threashold)
    {
        this.threashold=threashold;
        this.bufferedElements = new ArrayList<Tuple2<Integer, Integer>>();
    }
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
checkpointState.clear();
        for(Tuple2<Integer,Integer> element:bufferedElements){}
    }

    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {

    }

    public void invoke(Tuple2<Integer, Integer> value, Context context) throws Exception {
bufferedElements.add(value);
if(bufferedElements.size() == threashold)
{
    for(Tuple2<Integer,Integer> element:bufferedElements)
    {
        //
    }
    bufferedElements.clear();
}
    }
}
