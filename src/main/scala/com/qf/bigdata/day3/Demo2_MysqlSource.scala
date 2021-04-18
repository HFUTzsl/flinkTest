package com.qf.bigdata.day3

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

import org.apache.flink.configuration.Configuration
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.BeanProperty
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._


object Demo2_MysqlSource {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val dataStream: DataStream[Stu] = env.addSource(new MysqlSourceFunction)
    dataStream.print().setParallelism(1)
    env.execute("object Demo2_MysqlSource {")
  }
}

case class Stu(id:Int, name:String)

class MysqlSourceFunction extends RichParallelSourceFunction[Stu] {

  var ps:PreparedStatement = _
  var conn:Connection = _
  var rs:ResultSet = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://192.168.10.132:13307/spark"
    val username = "root"
    val password = "123"

    Class.forName(driver)
    try {
      conn = DriverManager.getConnection(url, username, password)
      val sql = "select * from stu"
      ps = conn.prepareStatement(sql)
    } catch {
      case exception: Exception => exception.printStackTrace()
    }
  }

  override def run(ctx: SourceFunction.SourceContext[Stu]): Unit = {
    try {
      rs = ps.executeQuery()
      while (rs.next()) {
        val stu:Stu = Stu(rs.getInt("id"), rs.getString("name"))
        ctx.collect(stu)
      }
    }catch {
      case exception: Exception => exception.printStackTrace()
    }
  }


  override def close(): Unit = {
    super.close()
    if (rs != null) rs.close()
    if (ps != null) ps.close()
    if (conn != null) conn.close()
  }

  override def cancel(): Unit = {}
}