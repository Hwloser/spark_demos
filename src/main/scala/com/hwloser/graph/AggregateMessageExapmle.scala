package com.hwloser.graph

import org.apache.spark.graphx.VertexRDD
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
      .take(10)
      .foreach(println)

    val olderFollowers: VertexRDD[(Int, Double)] = graph
      .aggregateMessages[(Int, Double)](
        triplet => {
          // Map Function
          if (triplet.srcAttr > triplet.dstAttr) {
            // send message to destination vertex containing counter and age
            triplet.sendToDst((1, triplet.srcAttr))
          }
        },
        // add counter and age
        (a, b) => {
          // reduce function
          (a._1 + b._1, a._2 + b._2)
        }
      )

    println("olderFollowers -- ")
    olderFollowers
      .cache()
      .takeOrdered(10)
      .foreach(println)

    // Divide total age by number of older followers
    // to get average of older followers
    val avgAgeOfOlderFollowers = olderFollowers
      .mapValues { (vertextId, vertexData) =>
        vertexData match {
          case (count, totalAge) => {
            totalAge / count
          }
        }
      }

    println("avgAgeOfOlderFollowers -- ")
    avgAgeOfOlderFollowers
      .take(10)
      .foreach(println)

    spark.stop()

  }
}
