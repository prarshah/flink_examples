package _3_state;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import javax.security.auth.login.Configuration;

public class _1_value_state {
    public static class Demo extends RichFlatMapFunction<Tuple2<Integer, String>, Long> {
        private transient ValueState<Long> sum;
        private transient ValueState<Long> count;
        //cannot be serialized and transferred // lost intentionally

        // open,close,gwtRunTimeContext,setRunTimeContext
            int ct =0;


        public void flatMap(Tuple2<Integer, String> integerStringTuple2, Collector<Long> collector) throws Exception {

            if(this.ct==0) {
                this.open(Configuration.getConfiguration());
                ct=ct+1;
            }
      //      System.out.println("Working");
            Long currCount= count.value();
            Long currSum = sum.value();

            currCount+=1;
            currSum= currSum + integerStringTuple2.f0;
            count.update(currCount);
            sum.update(currSum);

            if(currSum>=10)
            {
                collector.collect(sum.value());
                count.clear();
                sum.clear();
            }

            System.out.println(count.value()+ " "+ sum.value());

        }

        public void open(Configuration conf)
        {
            ValueStateDescriptor<Long> descriptor  = new ValueStateDescriptor<Long>("sum",
                    TypeInformation.of(new TypeHint<Long>() {}),0L);
            sum = getRuntimeContext().getState(descriptor);

            ValueStateDescriptor<Long> descriptor2  = new ValueStateDescriptor<Long>("count",
                    TypeInformation.of(new TypeHint<Long>() {}),0L);
            count = getRuntimeContext().getState(descriptor2);

        }

    }


    public static void main(String args[]) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        DataStream<Long> dat = env.socketTextStream("localhost",9999).map(new MapFunction<String, Tuple2<Integer, String>>() {
            public Tuple2<Integer, String> map(String s) {
                String words[] = s.split(",");
                return new Tuple2<Integer, String>(Integer.parseInt(words[1]), words[0]);
            }
        }).keyBy(0).flatMap(new Demo());

        System.out.println("\n\n\n\nDat1:");

        dat.print();
        env.execute("watermark");
    }
}