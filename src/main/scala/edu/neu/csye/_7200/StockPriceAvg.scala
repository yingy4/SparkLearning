package edu.neu.csye._7200

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object StockPriceAvg extends App {

  val sc = new SparkContext(new SparkConf().setAppName("StockPriceAvg").setMaster("local[*]"))

  def stockPriceAvg(lines: RDD[String]) = {
    lines.filter(x => x.split(",").head != "exchange")
        .map(x => (x.split(",")(1), x.split(",")(8).toDouble))
        .mapValues((_,1))
        .reduceByKey((x,y) => (x._1+y._1,x._2+y._2))
        .mapValues(x => x._1 / x._2 )
        .sortBy(_._2,false)
  }

  stockPriceAvg(sc.textFile("input//nyse//*")).take(10).foreach(println(_))

  sc.stop()
}
