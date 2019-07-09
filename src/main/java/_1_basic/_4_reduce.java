package _1_basic;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.taskmanager.TaskManager;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;



/*
fold is same as reduce
parameters are diffrent
 */


public class _4_reduce {
    public static void main(String args[]) throws Exception
    {
        String fp ="src/main/resources/";
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

/*
        1,John
        2,Albert
        3,Lui
        4,Smith
        5,Robert
*/

        DataSet<Tuple2<Integer,String>> df1 = env.readTextFile(fp+"person.txt").map(new MapFunction<String, Tuple2<Integer, String>>()
        {
            public Tuple2<Integer, String> map(String s) throws Exception {
                String ss[] = s.split(",");
                return new Tuple2<Integer,String>(Integer.parseInt(ss[0]),ss[1]);
            }
        });
        System.out.println("\n\n\n");
        df1.print();

        DataSet<Tuple2<Integer,String>> df2 = df1.map(new MapFunction<Tuple2<Integer, String>, Tuple2<Integer, String>>() {
            public Tuple2<Integer, String> map(Tuple2<Integer, String> t2) throws Exception {
                return new Tuple2<Integer,String>(t2.f0,t2.f1);
            }
            // reduce
        }).reduce(new ReduceFunction<Tuple2<Integer, String>>() {
            public Tuple2<Integer, String> reduce(Tuple2<Integer, String> pre, Tuple2<Integer, String> post) throws Exception {
                return new Tuple2<Integer, String>(pre.f0+post.f0,post.f1);
            }
        });


        System.out.println("\n\n\n");

        df2.print();

    }
}
