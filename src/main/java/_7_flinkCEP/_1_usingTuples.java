package _7_flinkCEP;

import _7_flinkCEP._72_practice.Alert;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class _1_usingTuples {
    public static void main(String args[]) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        //input stream
        DataStream<Tuple2<String,Integer>> input = env.socketTextStream("localhost", 9999).map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String x) throws Exception {
                String words[] = x.split(",");
                return new Tuple2<String,Integer>(words[0],Integer.parseInt(words[1]));
            }
        }
        )
                .keyBy(1);
;



        //pattern stream
        Pattern<Tuple2<String, Integer>, Tuple2<String, Integer>> patternStream = Pattern
                .<Tuple2<String,Integer>>begin("a", AfterMatchSkipStrategy.skipToNext())
        .where(new abc("a"))
        .followedBy("b")
        .where(new abc("b"))
        .timesOrMore(1)
        .optional()
        .next("c")
        .where(new abc("c"));


        System.out.println(patternStream.getAfterMatchSkipStrategy());



        // CEP

        SingleOutputStreamOperator<Alert> selectedAlp = CEP.pattern(input,patternStream)
                .select( (x)-> new Alert(x.toString()));
        selectedAlp.print();
        env.execute("abcCEP");
    }



public static class abc extends IterativeCondition<Tuple2<String,Integer>>
{
    String alp;
    abc(String a)
    {
        this.alp =a;
    }


    @Override
    public boolean filter(Tuple2<String, Integer> value, Context<Tuple2<String, Integer>> ctx) throws Exception {
        return value.f0.charAt(0)==alp.charAt(0);
    }
}

}
