
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

import scala.collection.mutable.ListBuffer

object Demo1_WordCount_scala {
  def main(args: Array[String]): Unit = {
    //1. 设置参数
    val port: Int = try {
      ParameterTool.fromArgs(args).getInt("port")
    } catch {
      case e: Exception => System.err.println("no port set, defaut port is 6666")
        6666
    }
    val hostname = "192.168.10.132"


    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val data: DataStream[String] = env.socketTextStream(hostname, port)

    val cnt: DataStream[WordCount] = data.flatMap(_.split("\\s+")).map(WordCount(_, 1L)).keyBy("word")
      .timeWindow(Time.seconds(2), Time.seconds(1)).sum("count")
    //      .reduce((a,b) => WordCount(a.word, a.count + b.count))


    cnt.print().setParallelism(1)
    env.execute("wordcount_scala")
  }

  case class WordCount(word: String, count: Long)

}