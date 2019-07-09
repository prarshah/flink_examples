package _6_stateBackend;


//******************************************************************************
//   If nothing else is configured, the system will use the MemoryStateBackend.


// use of asynchronous snapshots to avoid blocking pipelines

//size of each individual state is by default limited to 5 MB.

// rocksdb - https://ci.apache.org/projects/flink/flink-docs-release-1.8/ops/config.html#rocksdb-state-backend


/*

2 types of states

Managed State is represented in data structures controlled by the Flink runtime,
 such as internal hash tables, or RocksDB. Examples are “ValueState”, “ListState”, etc.
 Flink’s runtime encodes the states and writes them into the checkpoints.



Raw State is state that operators keep in their own data structures.
 When checkpointed, they only write a sequence of bytes into the checkpoint. Flink knows nothing about the state’s data structures and sees only the raw bytes.




 */


//******************************************************************************


import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.rocksdb.RocksDB;

import static java.lang.Thread.sleep;
//import org.junit.rules.TemporaryFolder;

public class _1_ {
    static {
        RocksDB.loadLibrary();
    }

    public static void main(String args[]) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


//(String checkpointDataUri,      boolean enableIncrementalCheckpointing)

        env.setStateBackend(new RocksDBStateBackend("file:///home/exa00105/PS/", true));
        env.enableCheckpointing(20);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(20);


        ExecutionConfig executionConfig = env.getConfig();
        System.out.println("Parallelism is:"+executionConfig.getParallelism());
        CheckpointConfig ckpConfig = env.getCheckpointConfig();
        System.out.println("Checkpoint mode is:"+ckpConfig.getCheckpointingMode());


       //
        //  env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        DataStream<Tuple2<Integer, Integer>> data = env.readTextFile("/home/exa00105/intint.txt")
                .map(new MapFunction<String, Tuple2<Integer, Integer>>() {
                    public Tuple2<Integer, Integer> map(String s) throws Exception {
                        String w[] = s.split(",");
                        return new Tuple2<Integer, Integer>(Integer.parseInt(w[0]), Integer.parseInt(w[1]));
                    }
                })
                .keyBy(0).sum(1);
        sleep(100);

        data.print();
        env.execute("allStates");
    }
}
