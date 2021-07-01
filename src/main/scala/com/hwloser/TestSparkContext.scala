package com.hwloser

import org.apache.spark.sql.SparkSession

object TestSparkContext {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("test spark session")
      .master("local[*]")
      .getOrCreate()

  }
}
