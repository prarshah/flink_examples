package _1_basic;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;









//    This

//      is

//      depricated







public class _6_split {
        public static void main(String args[]) throws Exception
        {
            String fp = "src/main/resources/";
            final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

            DataStreamSource<String> df1 = env.readTextFile(fp+"oddeven.txt");


            SplitStream<Integer> sp = df1.map((x)->Integer.parseInt(x)).setParallelism(1)
                    .split(new OutputSelector<Integer>() {                // Interface to return tagged elements or obj of splitstream
                        public Iterable<String> select(Integer integer) { //select method to lable .. returns labled iterable
                            List<String> out = new ArrayList<String>();
                            if(integer%2 == 0)
                                out.add("even");
                            else
                                out.add("odd");
                            //out.add(integer % 2 == 0 ? "even" : "odd");
                            //System.out.println(out.size()+1);
                            //System.out.println("ummm");
                            return out;
                        }
                    });

           DataStream<Integer> even =  sp.select("even");
           even.writeAsText(fp+"/split_output/output_even", FileSystem.WriteMode.OVERWRITE).name("even.txt").setParallelism(1);

            DataStream<Integer> odd =sp.select("odd");
            odd.writeAsText(fp+"/split_output/output_odd", FileSystem.WriteMode.OVERWRITE).name("odd.txt").setParallelism(1);

            env.execute("abc");

        }
    }

