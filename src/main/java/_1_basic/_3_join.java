package _1_basic;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

public class _3_join {
    public static void main(String args[]) throws Exception
    {

        // or StreamExecutionEnviorment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // if you want to use parameters ,give global parameters
        //final ParameterTool params = ParameterTool.fromArgs(args);

        // give parameters to web interface
        //env.getConfig().setGlobalJobParameters(params);

        // read and map tuples from it
        String fp ="src/main/resources/";
        final DataSet<Tuple2<Integer,String>> df1 = env.readTextFile(fp+"person.txt").map(new MapFunction<String, Tuple2<Integer, String>>() {
            public Tuple2<Integer, String> map(String s) throws Exception {
                String ss[] = s.split(",");

                return new Tuple2<Integer,String>(Integer.parseInt(ss[0]),ss[1]);
            }
        });


        System.out.println("\n\n\n");
        df1.print();




        final DataSet<Tuple2<Integer,String>> df2 = env.readTextFile(fp+"location.txt").map(new MapFunction<String, Tuple2<Integer, String>>() {
            public Tuple2<Integer, String> map(String s) throws Exception {
                String ss[] = s.split(",");

                return new Tuple2<Integer,String>(Integer.parseInt(ss[0]),ss[1]);
            }
        });

        System.out.println("\n\n\n");
        df2.print();


        // inner join
        DataSet<Tuple3<Integer,String,String>> joined = df1.join(df2).where(0).equalTo(0).with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {
            public Tuple3<Integer, String, String> join(Tuple2<Integer, String> df_one, Tuple2<Integer, String> df_two) throws Exception {
                return new Tuple3<Integer,String,String>(df_one.f0, df_one.f1, df_two.f1);
            }
        });
        System.out.println("\n\n\nInner Join: \n");
        joined.print();


        // left outer join
        DataSet<Tuple3<Integer,String,String>> lo_joined = df1.leftOuterJoin(df2).where(0).equalTo(0).with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {
            public Tuple3<Integer, String, String> join(Tuple2<Integer, String> df_one, Tuple2<Integer, String> df_two) throws Exception {
                if(df_two == null)
                    return new Tuple3<Integer,String,String>(df_one.f0, df_one.f1, null);
                return new Tuple3<Integer,String,String>(df_one.f0, df_one.f1, df_two.f1);
            }
        });
        System.out.println("\n\n\nLeft Outer Join: \n");
        lo_joined.print();




        // right outer join
        DataSet<Tuple3<Integer,String,String>> ro_joined = df1.rightOuterJoin(df2).where(0).equalTo(0).with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {
            public Tuple3<Integer, String, String> join(Tuple2<Integer, String> df_one, Tuple2<Integer, String> df_two) throws Exception {
                if(df_one == null)
                    return new Tuple3<Integer,String,String>(df_two.f0, null, df_two.f1);
                return new Tuple3<Integer,String,String>(df_one.f0, df_one.f1, df_two.f1);
            }
        });
        System.out.println("\n\n\nRight Outer Join: \n");
        ro_joined.print();


        try{env.execute();}
        catch(Exception e){}
    }
}
