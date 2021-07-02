package com.hwloser.utils

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx.GraphXUtils

trait LocalSparkContext {

  def withSparkContext[T](clazz: Class[_])
                         (f: SparkContext => T): Unit = {
    withSparkSession(clazz)(ss => f(ss.sparkContext))
  }

  def withSparkSession[T](clazz: Class[_])
                         (f: SparkSession => T): Unit = {
    val conf = sparkConf(clazz.getSimpleName, "local[*]")
    GraphXUtils.registerKryoClasses(conf)
    val ss = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()
    try {
      f(ss)
    } finally {
      ss.stop()
    }
  }

  def sparkConf(appName: String, master: String): SparkConf = {
    new SparkConf()
      .setAppName(appName)
      .setMaster(master)
  }

}
