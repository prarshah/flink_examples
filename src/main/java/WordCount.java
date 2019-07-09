
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;

/*public class WordCount
{
    public static void main(String[] args)
            throws Exception
    {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        ParameterTool params = ParameterTool.fromArgs(args);

        env.getConfig().setGlobalJobParameters(params);

        DataSet<String> text = env.readTextFile("/home/exa00105/wc.txt");

        DataSet<String> filtered = text.filter(new FilterFunction<String>()

        {
            public boolean filter(String value)
            {
                return value.startsWith("N");
            }
        });
        DataSet<Tuple2<String, Integer>> tokenized = filtered.map(new Tokenizer());

        DataSet<Tuple2<String, Integer>> counts = tokenized.groupBy(new int[] { 0 }).sum(1);
            counts.writeAsCsv("/home/exa00105/abcd.txt", "\n", " ");
            env.execute("WordCount Example");
     }

    public static final class Tokenizer
            implements MapFunction<String, Tuple2<String, Integer>>
    {
        public Tuple2<String, Integer> map(String value)
        {
            return new Tuple2(value, Integer.valueOf(1));
        }
}    }*/
