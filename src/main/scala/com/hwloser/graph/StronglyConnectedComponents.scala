package com.hwloser.graph

import com.hwloser.utils.LocalSparkContext
import org.apache.spark.graphx.{Edge, PartitionStrategy}
import org.apache.spark.graphx.impl.GraphImpl
import org.apache.spark.storage.StorageLevel

object StronglyConnectedComponents extends LocalSparkContext {

  def main(args: Array[String]): Unit = withSparkContext(getClass) { sc =>
    val rawEdges =
      sc.parallelize((0L to 6L).map(x => (x, (x + 1) % 7)))

    // generate edges RDD
    val edges = rawEdges.map { case (srcId, dstId) =>
      Edge(srcId, dstId, 1)
    }

    // generate graph from edges
    val graph = {
      GraphImpl(edges, -1, StorageLevel.MEMORY_ONLY, StorageLevel.MEMORY_ONLY)
        .partitionBy(PartitionStrategy.EdgePartition2D)
        .groupEdges((a, b) => a + b)
        .cache()
    }

    graph.vertices.foreach { v =>
      println(s"${Thread.currentThread()} -- v: ${v}")
    }

    graph.edges.foreach { e =>
      println(s"${Thread.currentThread()} -- e: ${e}")
    }

  }
}
