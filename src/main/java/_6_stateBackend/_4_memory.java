package _6_stateBackend;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;



//A default state backend can be configured in the flink-conf.yaml, using the configuration key state.backend.



import java.util.List;

public class _4_memory {

    public static class Func extends RichFlatMapFunction<Tuple2<Integer,Integer>,String>
    {

        static transient ValueState<Integer> count;
        static transient ListState<Integer> nums;
        static transient ReducingState<Integer> sum;
        static int ct=0;

        public void flatMap(Tuple2<Integer,Integer> t2, Collector<String> out) throws Exception {
            if(this.ct == 0) {
                this.open(++ct);
                count.update(0);
            }

            if(t2.f0==null || t2.f1==null)
            {System.out.println("null,null");
                return;
            }


            List<Integer> a = null;

            count.update(count.value()+1);

            nums.add(t2.f0);

            sum.add(t2.f1);

            String all = "Count: "+count.value()+" Sum: "+sum.get()+" Numbers: ";
            for (Integer i: nums.get())
            {
                all=all+i+",";
            }

            out.collect(all);

        }

        void open(int c)
        {

            ValueStateDescriptor<Integer> vsd = new ValueStateDescriptor<Integer>("count",Integer.class,0);
            count=getRuntimeContext().getState(vsd);


            ListStateDescriptor<Integer> lsd = new ListStateDescriptor<Integer>("nums", Integer.class  );
            nums= getRuntimeContext().getListState(lsd);

            ReducingStateDescriptor<Integer> rsd = new ReducingStateDescriptor<Integer>("sum",new SumReduce(),Integer.class);

            sum=getRuntimeContext().getReducingState(rsd);

        }

    }





    public static class SumReduce implements ReduceFunction<Integer>
    {

        public Integer reduce(Integer t2, Integer t1) throws Exception
        {
            return (t2+t1);
        }
    }





    public static void main(String args[])
    {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        System.out.println("Default checkpointing (is Enabled?): "+env.getCheckpointConfig().isCheckpointingEnabled());
        env.enableCheckpointing(100);
        System.out.println("Enabled?: "+env.getCheckpointConfig().isCheckpointingEnabled());

        System.out.println("Checkpointing mode : "+env.getCheckpointConfig().getCheckpointingMode());
        System.out.println("State Backend  : "+env.getStateBackend());
        env.setStateBackend(new MemoryStateBackend());
        System.out.println("Now State Backend  : "+env.getStateBackend());

        DataStream<Tuple2<Integer,Integer>> data = env.socketTextStream("localhost",9999)
                .map(new MapFunction<String, Tuple2<Integer,Integer>>() {
                    public Tuple2<Integer, Integer> map(String s) throws Exception {
                        String w[] = s.split(",");
                        return new Tuple2<Integer, Integer>(Integer.parseInt(w[0]),Integer.parseInt(w[1]));
                    }
                });

        DataStream<String> data2 = ((SingleOutputStreamOperator<Tuple2<Integer, Integer>>) data).setParallelism(3).keyBy(0).flatMap(new Func());

        data2.print();

        try{  env.execute("anc");}
        catch(Exception e) {
//e.printStackTrace();
        }

    }
}
