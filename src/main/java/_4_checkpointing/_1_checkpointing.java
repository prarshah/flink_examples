package _4_checkpointing;

import _3_state._3_reducing_state;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.executiongraph.restart.RestartStrategy;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class _1_checkpointing {

    public static void main(String args[]) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final RocksDBStateBackend rocksdb = new RocksDBStateBackend(" file:///home/exa00105/PS/", true);
        env.setStateBackend(rocksdb);

        env.enableCheckpointing(1000);

        //between next trigger for checkpointing
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(700);

        //time to complete checkpointing
        env.getCheckpointConfig().setCheckpointTimeout(1000);

        //garuntee level - exactly / atleast once   //atleast takes less time
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        //allow only 1 checkpoint processing at a time
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        // external checkpoints aftr computation are deleted ...
        // 2option -- retain or delete on cancellation
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);

        // restart statergy method
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,10));
        //or
        // failure rate restart statergy -till failure rate exceeds
        //no restart statergy
        //fallback

        DataStream<Tuple2<Integer, Integer>> data = env.readTextFile("/home/exa00105/intint.txt")
                .map(new MapFunction<String, Tuple2<Integer, Integer>>() {
                    public Tuple2<Integer, Integer> map(String s) throws Exception {
                        String w[] = s.split(",");
                        return new Tuple2<Integer, Integer>(Integer.parseInt(w[0]), Integer.parseInt(w[1]));
                    }
                });

        data.print();
        env.execute("allStates");
    }
}
