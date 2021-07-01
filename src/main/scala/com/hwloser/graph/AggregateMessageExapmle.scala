package com.hwloser.graph

import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.sql.SparkSession



object AggregateMessageExapmle {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName(s"${getClass.getSimpleName}")
      .getOrCreate()

    val sc = spark.sparkContext

    val graph = GraphGenerators
      .logNormalGraph(sc, numVertices = 100)
      .mapVertices((id, _) => id.toDouble)

    graph
      .cache()
      .triplets
      .collect()
      .foreach(println)



  }
}
