package _1_basic;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class _2_ip_op_ways {

    public static void main(String args[])
    {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // input method 2
    DataStream<String> words = env.socketTextStream("localhost", 9999);
    final DataStream<Tuple2<String,Integer>> count= words.map(new MapFunction<String, Tuple2<String, Integer>>() {
        public Tuple2<String, Integer> map(String s) throws Exception {
            return new Tuple2<String,Integer>(s,1);
        }
    }).keyBy(0).sum(1);
    // use keyBy instead of groupBy  for streaming

    count.print();
    // input method 3
    //DataStream<String> lines = env.readTextFile("file:///path");

    //Basic stream sinks
    //method 1
        //adults.print()
    //mrthod 2
        //stream.writeAsText("/path/to/file") or CSV
    //method 3
        //stream.writeToSocket(host, port, SerializationSchema)
        try {
            env.execute("Streaming WordCount");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
