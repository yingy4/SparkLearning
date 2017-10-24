package edu.neu.csye._7200

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount extends App {

  val sc = new SparkContext(new SparkConf().setAppName("WordCount").setMaster("local[*]"))

  def wordCount(lines: RDD[String],separator: String) = {
    lines.flatMap(_.split(separator))
         .map((_,1))
         .reduceByKey(_ + _)
  }

  wordCount(sc.textFile("input//WordCount.txt")," ").collect().foreach(println(_))

  sc.stop()
}
