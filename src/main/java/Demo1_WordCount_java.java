
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 窗口操作来进行wc
 */
public class Demo1_WordCount_java {
    public static void main(String[] args) throws Exception {
        //1. 获取到参数对象
        int port;
        try {
            ParameterTool parameterTool = ParameterTool.fromArgs(args);
            port = parameterTool.getInt("port");
        } catch (Exception e) {
            System.err.println("no port set, defaut port is 6666");
            port = 6666;
        }
        String hostname = "192.168.10.132";

        //2. 获取到flink的对象
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> data = env.socketTextStream(hostname, port);

        //3. wordcount
        SingleOutputStreamOperator<WordCount> pairWords = data.flatMap(new FlatMapFunction<String, WordCount>() {
            @Override
            public void flatMap(String value, Collector<WordCount> out) throws Exception {
                String[] splits = value.split(" ");
                for (String word : splits) {
                    out.collect(new WordCount(word, 1L));
                }
            }
        });
        // 将元组按照key进行分组
        KeyedStream<WordCount, Tuple> grouped = pairWords.keyBy("word");
        WindowedStream<WordCount, Tuple, TimeWindow> window = grouped.timeWindow(Time.seconds(2), Time.seconds(1));
        SingleOutputStreamOperator<WordCount> cnts = window.sum("count");

        cnts.print().setParallelism(1);
        env.execute("wordcount");
    }

    public static class WordCount {
        public String word;
        public Long count;

        public WordCount() {
        }

        public WordCount(String word, Long count) {
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString() {
            return "WordCount{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }
    }
}