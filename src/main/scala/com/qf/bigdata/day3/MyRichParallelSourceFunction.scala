import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

import scala.util.Random
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration

import scala.util.Random

class MyRichParallelSourceFunction extends RichParallelSourceFunction[String] {
  override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
    val random = new Random()
    while (true) {
      val num: Int = random.nextInt(100)
      ctx.collect(s"random_rich[${num}]")
      Thread.sleep(500)
    }
  }

  override def cancel(): Unit = {

  }

  /**
    * 初始化
    */
  override def open(parameters: Configuration): Unit = super.open(parameters)

  /**
    * 适合释放资源的时候
    */
  override def close(): Unit = super.close()
}