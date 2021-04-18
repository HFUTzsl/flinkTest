import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
  * @Author shanlin
  * @Date Created in  2021/1/21 17:37
  *
  */
object test {
  def main(args: Array[String]): Unit = {
    // 从外部命令中获取参数
    //    val params: ParameterTool = ParameterTool.fromArgs(args)
    //    val host: String = params.get("host")
    //    val port: Int = params.getInt("port")
    // 创建流处理环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 接收 socket 文本流
    val textDstream: DataStream[String] = env.socketTextStream("192.168.10.132", 7777)
    // flatMap 和 Map 需要引用的隐式转换
    import org.apache.flink.api.scala._
    val dataStream: DataStream[(String, Int)] =
      textDstream.flatMap(_.split(" ")).filter(_.nonEmpty).map((_, 1)).keyBy(0).sum(1)
    dataStream.print().setParallelism(1)
    // 启动 executor，执行任务
    env.execute("Socket stream word count")
  }
}
