package _1_basic;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class _5_aggreation {
    public static void main(String args[]) throws Exception
    {
        String fp = "src/main/resources/";
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
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
        });

        System.out.println("\n\n\nSum:\n");
        df2.sum(0).print();

        System.out.println("\n\n\nMin:\n");
        df2.min(0).print();

        System.out.println("\n\n\nMax:\n");
        df2.max(0).print();

        System.out.println("\n\n\nMinBy:\n");
        df2.minBy(0).print();

        System.out.println("\n\n\nMaxBy:\n");
        df2.maxBy(0).print();

        System.out.println("\n\n\n");
        df2.print();
    }
}