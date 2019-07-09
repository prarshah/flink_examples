package _7_flinkCEP._72_practice;

import _7_flinkCEP._72_practice.Alert;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.List;
import java.util.Map;


public class MyNFA {


    public static void main(String args[]) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        //input stream
        DataStream<Alphabets> alphabets = env.socketTextStream("localhost", 9999).map((x) -> new Alphabets(x));//.keyBy(0);




        //pattern stream
        System.out.println("\npattern1\n");
        Pattern<Alphabets, ?> patternStream = Pattern
                .<Alphabets>begin("a",AfterMatchSkipStrategy.skipToNext())
                                        .where(new abc("a"))
                .followedByAny("b")        .where(new abc("b"))         .timesOrMore(1)                  .optional()
                .next("c")              .where(new abc("c"))            ;


        System.out.println("\npattern2\n");
        //pattern stream
        Pattern<Alphabets, ?> patternStream1 = Pattern
                .<Alphabets>begin("a1",AfterMatchSkipStrategy.skipToFirst("a1"))
                .where(new abc("a"))
                .next("c1")                 .where(new abc("c"))
                .next("c2")                 .where(new abc("c"))
                ;

        System.out.println(patternStream.getAfterMatchSkipStrategy());

        // CEP
        System.out.println("\ncep1\n");
        SingleOutputStreamOperator<Alert> selectedAlp = CEP.pattern(alphabets,patternStream)
                .select( (x)-> new Alert(x.toString()));


        System.out.println("\ncep2\n");
        SingleOutputStreamOperator<Alert> selectedAlp1 = CEP.pattern(alphabets,patternStream1)
                .select( (x)-> new Alert(x.toString()));


        System.out.println("\nprint1\n");
        selectedAlp.print();

        System.out.println("\nprint2\n");
        selectedAlp1.print();
        env.execute("abcCEP");
    }
}



class abc extends IterativeCondition<Alphabets>
        {
            char alp;
abc(String a)
{
    this.alp =a.charAt(0);
}

            @Override
            public boolean filter(Alphabets value, Context<Alphabets> ctx) throws Exception {
                return value.getAlphabet() == alp;

            }
        }
