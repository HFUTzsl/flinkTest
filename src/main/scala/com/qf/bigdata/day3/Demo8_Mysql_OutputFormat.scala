package com.qf.bigdata.day3

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

import org.apache.flink.api.common.io.OutputFormat
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._


object Demo8_Mysql_OutputFormat {
  def main(args: Array[String]): Unit = {
    //1. 处理数据
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val dataStream: DataStream[Emp] = env.socketTextStream("192.168.10.132", 6666)
      .map(line => {
        val fields: Array[String] = line.split("\\s+")
        println(fields.mkString(","))
        Emp(fields(0).toInt, fields(1), fields(2).toDouble, fields(3))
      })
    dataStream.print.setParallelism(1)
    //2. 输出
    dataStream.writeUsingOutputFormat(new CustomMySqlOutputFormat)
    //3. 启动
    env.execute("Demo8_Mysql_OutputFormat")
  }
}

case class Emp(id: Int, name: String, salary: Double, address: String)

class CustomMySqlOutputFormat extends OutputFormat[Emp] {

  var ps: PreparedStatement = _
  var conn: Connection = _
  var rs: ResultSet = _

  /**
    * 用于配置相关的初始化
    */
  override def configure(parameters: Configuration): Unit = {}

  /**
    * 业务初始化
    */
  override def open(taskNumber: Int, numTasks: Int): Unit = {
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://localhost:3306/dw"
    val username = "root"
    val password = "admin"

    Class.forName(driver)
    try {
      conn = DriverManager.getConnection(url, username, password)
    } catch {
      case exception: Exception => exception.printStackTrace()
    }
  }

  /**
    * 写数据
    */
  override def writeRecord(emp: Emp): Unit = {
    ps = conn.prepareStatement("insert into emp values(?, ?, ?, ?)")
    ps.setInt(1, emp.id)
    ps.setString(2, emp.name)
    ps.setDouble(3, emp.salary)
    ps.setString(4, emp.address)
    ps.execute()
  }

  /**
    * 最后被调用
    */
  override def close(): Unit = {
    if (rs != null) rs.close()
    if (ps != null) ps.close()
    if (conn != null) conn.close()
  }
}