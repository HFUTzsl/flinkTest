package com.qf.bigdata.day4

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

object Demo2 {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val outputPath = "C:\\123"
    // get input data
    val text = env.readTextFile("C:\\resource\\data.txt")

    val counts = text.flatMap {
      _.toLowerCase.split("\\W+") filter {
        _.nonEmpty
      }
    }
      .map {
        (_, 1)
      }
      .groupBy(0)
      .sum(1)

    counts.print()
    counts.writeAsCsv(outputPath, "\n", ",")
    env.execute("jobName")
  }
}