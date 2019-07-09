package _2_Windows;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.evictors.CountEvictor;
import org.apache.flink.streaming.api.windowing.time.Time;

public class _1_tumbling_windows_with_evictor {
public static void main(String args[]) throws Exception
{
    class Redf implements ReduceFunction<Tuple5<String,String,String,Integer,Integer>>
    {

        public Tuple5<String, String, String, Integer, Integer> reduce(Tuple5<String, String, String, Integer, Integer> pre, Tuple5<String, String, String, Integer, Integer> post) throws Exception {
            return new Tuple5<String,String,String,Integer,Integer>(pre.f0,pre.f1,pre.f2,pre.f3+post.f3,pre.f4+post.f4);
        }
    }

    class Spli implements MapFunction<String,Tuple5<String,String,String,Integer,Integer>>
    {
        public Tuple5<String,String,String,Integer,Integer> map(String value)
        {
            String words[] = value.split(",");
            return new Tuple5<String,String,String,Integer,Integer>(words[0],words[1],words[2],Integer.parseInt(words[3]),Integer.parseInt(words[4]));
        }
    }

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);   // in 3rd phase -processing phase -diff for diff machine
    // evantTime          // in 1st phasse-source --is present in record
    // Ingetion time          // in 1st phasse-source --when it is in source or enters flink

    DataStream<String> data = env.socketTextStream("localhost",9999);
    DataStream<Tuple5<String,String,String,Integer,Integer>> mapped = data.map(new Spli());


    //tumbling window
    DataStream<Tuple5<String,String,String,Integer,Integer>> reduced = mapped.keyBy(0)
    .window(TumblingProcessingTimeWindows.of(Time.seconds(2)))
            .evictor(CountEvictor.of(3))
    .reduce(new Redf());

    reduced.print();
    env.execute("tumblingWindow");
}

}
