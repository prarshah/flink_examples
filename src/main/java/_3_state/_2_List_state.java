package _3_state;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import javax.security.auth.login.Configuration;

public class _2_List_state {

    public static void main(String args[]) throws Exception
    {




        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        DataStream<Long> dat = env.socketTextStream("localhost",9999).map(new MapFunction<String, Tuple2<Integer, String>>() {
            public Tuple2<Integer, String> map(String s) {
                String words[] = s.split(",");
                return new Tuple2<Integer, String>(Integer.parseInt(words[1]), words[0]);
            }
        }).keyBy(0).flatMap(new _1_value_state.Demo());

        System.out.println("\n\n\n\nDat1:");
        dat.print();
        env.execute("watermark");






    }

    public static class MyClass extends RichFlatMapFunction<Tuple2<Integer,Integer>,String>{

        private transient ValueState<Long> count;
        private transient ListState<Long> numbers;

        public void flatMap(Tuple2<Integer, Integer> integerIntegerTuple2, Collector<String> collector) throws Exception {
        Long curVal = Long.parseLong(integerIntegerTuple2.f0+"");
        Long curCount =count.value();

        curCount +=1;


        count.update(curCount);
        numbers.add(curVal);

        String outStr ="";
        for(Long i:  numbers.get())
        {
            outStr+=i;
        }

        collector.collect(outStr);
        numbers.clear();
        }
        public void open(Configuration conf)
        {
            ListStateDescriptor<Long>  listDesc = new ListStateDescriptor<Long>("numbers",Long.class);
                    numbers=getRuntimeContext().getListState(listDesc);

            ValueStateDescriptor<Long> valst = new ValueStateDescriptor<Long>("value",Long.class);
            count=getRuntimeContext().getState(valst);

        }
    }
}
