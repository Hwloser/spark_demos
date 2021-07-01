package com.hwloser.graph

import org.apache.spark.sql.SparkSession

class StronglyConnectedComponents {

  def main(args: Array[String]): Unit = {
    val ss = SparkSession
      .builder()
      .appName(s"${getClass.getSimpleName}")
      .master("local[*]")
      .getOrCreate()

    val sc = ss.sparkContext


  }
}
