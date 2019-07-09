package _1_basic;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;

/*

test1
        1,1
        1,1
        1,1
        1,1
        1,1
test2
        2,2
        2,2
        2,2
        2,2

*/

public class _8_connect {
    public static void main(String args[]) throws Exception
    {
        String fp = "src/main/resources/";

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<Integer,Integer>> data = env.readTextFile(fp+"test1")
                .map(new MapFunction<String, Tuple2<Integer,Integer>>() {
                    public Tuple2<Integer, Integer> map(String s) throws Exception {
                        String w[] = s.split(",");
                        return new Tuple2<Integer, Integer>(Integer.parseInt(w[0]),Integer.parseInt(w[1]));
                    }
                });


        DataStream<Tuple2<Integer,Integer>> data1 = env.readTextFile(fp+"test2")
                .map(new MapFunction<String, Tuple2<Integer,Integer>>() {
                    public Tuple2<Integer, Integer> map(String s) throws Exception {
                        String w[] = s.split(",");
                        return new Tuple2<Integer, Integer>(Integer.parseInt(w[0]),Integer.parseInt(w[1]));
                    }
                });

        ConnectedStreams<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> datac = data.connect(data1);
        datac.keyBy(0,0)
        .flatMap(new CoFlatMapFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer,Integer>>( ) {
            @Override
            public void flatMap1(Tuple2<Integer, Integer> value, Collector<Tuple2<Integer, Integer>> out) throws Exception {
            out.collect(new Tuple2<Integer,Integer>(value.f0+5,value.f1+5));
            }

            @Override
            public void flatMap2(Tuple2<Integer, Integer> value, Collector<Tuple2<Integer, Integer>> out) throws Exception {
                out.collect(value);
            }
        }).print();

        env.execute();

    }

}
