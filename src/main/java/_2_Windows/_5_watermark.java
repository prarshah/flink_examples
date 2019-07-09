package _2_Windows;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import javax.annotation.Nullable;
import java.util.concurrent.TimeUnit;


// implements assigner with puncuated watermarks
public class _5_watermark {

    public static class Demo implements AssignerWithPeriodicWatermarks<Tuple2<Integer, String>> {
        final long allowedLateTime = 3500;
        private long currentMaxTimesStamp = 0;

        @Nullable
        public Watermark getCurrentWatermark() {
            return new Watermark(currentMaxTimesStamp - allowedLateTime);
        }

        public long extractTimestamp(Tuple2<Integer, String> e2, long l) {
            long timeStamp = e2.f0;
            currentMaxTimesStamp = Math.max(timeStamp, currentMaxTimesStamp);
            System.out.println("Timestamp for "+ e2.f1+" is "+ timeStamp);
            return timeStamp;
        }
    }

    public static void main(String args[]) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple2<Integer, String>> dat = env.readTextFile("/home/exa00105/PS/count3.txt").map(new MapFunction<String, Tuple2<Integer, String>>() {
            public Tuple2<Integer, String> map(String s) throws Exception {
                String words[] = s.split(",");
                return new Tuple2<Integer, String>(Integer.parseInt(words[1]), words[0]);
            }
        });



        //custom watermark generator
        DataStream<Tuple2<Integer, String>> dat1    =   dat.assignTimestampsAndWatermarks(new Demo()).
                windowAll(EventTimeSessionWindows.withGap(Time.of(10, TimeUnit.SECONDS))).reduce(
                new ReduceFunction<Tuple2<Integer, String>>() {
                    public Tuple2<Integer, String> reduce(Tuple2<Integer, String> t2, Tuple2<Integer, String> t1) throws Exception {
                        return new Tuple2<Integer, String>(t2.f0+t1.f0,t2.f1+","+t1.f1);
                    }
                });




        //builtin watermarks generator


        DataStream<Tuple2<Integer, String>> dat2     =  dat
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple2<Integer, String>>(Time.seconds(5)) {
            @Override
            public long extractTimestamp(Tuple2<Integer, String> integerStringTuple2) {
                return 0;
            }
        }).
                windowAll(EventTimeSessionWindows.withGap(Time.of(10, TimeUnit.SECONDS))).reduce(
                new ReduceFunction<Tuple2<Integer, String>>() {
                    public Tuple2<Integer, String> reduce(Tuple2<Integer, String> t2, Tuple2<Integer, String> t1) throws Exception {
                        return new Tuple2<Integer, String>(t2.f0+t1.f0,t2.f1+","+t1.f1);
                    }
                });




        System.out.println("\n\n\n\nDat1:");
        dat2.print();
        env.execute("watermark");

    }
}