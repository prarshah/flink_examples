package _1_basic;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class _7_iterate_operator  {

public static void main(String args[]) throws Exception
{
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    DataStream<Tuple2<Long,Integer>> in= env.generateSequence(0,5).map(new MapFunction<Long, Tuple2<Long, Integer>>() {
        public Tuple2<Long, Integer> map(Long a) throws Exception {
            return new Tuple2<Long,Integer>(a,0);
        }
    });
    IterativeStream<Tuple2<Long,Integer>> iterativeStream = in.iterate(1000);

    DataStream<Tuple2<Long,Integer>> pulseOne = iterativeStream.map(new MapFunction<Tuple2<Long, Integer>, Tuple2<Long, Integer>>() {
        public Tuple2<Long, Integer> map(Tuple2<Long, Integer> value) throws Exception {
            if(value.f0 >= 10)
                return value;
            else
                return new Tuple2<Long,Integer>(value.f0+1,value.f1+1);
        }
    });


    DataStream<Tuple2<Long,Integer>> not10s = pulseOne.filter(new FilterFunction<Tuple2<Long, Integer>>() {
        public boolean filter(Tuple2<Long, Integer> value) throws Exception {
            return !(value.f0==10);
        }
    });

    iterativeStream.closeWith(not10s);


    DataStream<Tuple2<Long,Integer>> all_10s = pulseOne.filter(new FilterFunction<Tuple2<Long, Integer>>() {
        public boolean filter(Tuple2<Long, Integer> value) throws Exception {
            return (value.f0==10);
        }
    });

    all_10s.print();

    env.execute();



}

}
