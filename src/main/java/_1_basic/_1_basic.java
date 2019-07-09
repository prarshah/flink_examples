package _1_basic;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class _1_basic {

    public static class Person {
        public String name;
        public Integer age;
        public Person() {}

        public Person(String name, Integer age) {
            this.name = name;
            this.age = age;
        }

        @Override
        public String toString() {
            return this.name.toString() + ": age " + this.age.toString();
        }
    }


    public static  void main(String args[]) throws Exception
{
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    DataStream<Person> person = env.fromElements(
            new Person("Fred", 35),
            new Person("Wilma", 15),
            new Person("Pebbles", 2)
                                                );
    DataStream<Person> adults = person.filter(new FilterFunction<Person>() {
        public boolean filter(Person person) throws Exception {
            return person.age>12;
        }
    });
adults.print();
env.execute();

System.out.println("end");
}
}
