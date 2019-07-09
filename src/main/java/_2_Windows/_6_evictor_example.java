package _2_Windows;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;

import java.util.Iterator;


/*


1,1,1,1,1,1
8,8,8,8,8,8
3,3,3,3,3,3
1,1,1,1,1,1
1,1,1,1,1,1
1,1,1,1,1,1


* */

public class _6_evictor_example { public static void main(String args[]) throws Exception
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
                    .evictor(new Evictor<Tuple5<String, String, String, Integer, Integer>, TimeWindow>() {
                        @Override

                      // remove values below 5


                        public void evictBefore(Iterable<TimestampedValue<Tuple5<String, String, String, Integer, Integer>>> elements, int size, TimeWindow window, EvictorContext evictorContext) {

                            for (Iterator<TimestampedValue<Tuple5<String, String, String, Integer, Integer>>> it = elements.iterator(); it.hasNext(); ) {
                                TimestampedValue<Tuple5<String, String, String, Integer, Integer>> element = it.next();
                                if(element.getValue().f3 < 5 )
                                    it.remove();

                            }
                        }

                        @Override
                        public void evictAfter(Iterable<TimestampedValue<Tuple5<String, String, String, Integer, Integer>>> elements, int size, TimeWindow window, EvictorContext evictorContext) {

                        }
                    })
                    .reduce(new Redf());

            reduced.print();
           try
           {
               env.execute("tumblingWindow");
           }
           catch(Exception e)
           {

           }
        }
}

