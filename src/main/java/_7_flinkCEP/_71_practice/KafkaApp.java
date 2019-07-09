/*
package _7_flinkCEP._8_practice;

import java.util.List;
import java.util.Map;

import _7_flinkCEP.Alert;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

public class KafkaApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<TemperatureEvent> inputEventStream = env.socketTextStream("localhost",9999).map(new MapFunction<String, TemperatureEvent>() {
            @Override
            public TemperatureEvent map(String value) throws Exception {
                String words[] = value.split(",");
                TemperatureEvent o=new TemperatureEvent(words[0]);
                o.setTemperature(Double.parseDouble(words[1]));
                return o;
            }
        });

        Pattern<TemperatureEvent, ?> warningPattern = Pattern.<TemperatureEvent> begin("first")
                .subtype(TemperatureEvent.class).where(new IterativeCondition<TemperatureEvent>() {


                    private static final long serialVersionUID = 1L;
                    @Override
                    public boolean filter(TemperatureEvent value, Context<TemperatureEvent> ctx) throws Exception {
                        if (value.getTemperature() >= 26.0) {
                            return true;
                        }
                        return false;
                    }
                }).within(Time.seconds(10));



        DataStream<Alert> patternStream = CEP.pattern(inputEventStream, warningPattern)
                .select(new PatternSelectFunction<TemperatureEvent, Alert>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Alert select(Map<String, List<TemperatureEvent>> event) throws Exception {

                        return new Alert("Temperature Rise Detected:" + event.get(0).get(0).getTemperature()
                                + " on machine name:" + event.get(0).get(0).getMachineName());
                    }

                });



        patternStream.print();
        env.execute("CEP on Temperature Sensor");
    }
}*/
