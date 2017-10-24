package edu.neu.csye._7200

import org.apache.spark.rdd.RDD
import org.apache.spark.util.StatCounter
import org.apache.spark.{SparkConf, SparkContext}

object StockPriceStat extends App {

  val sc = new SparkContext(new SparkConf().setAppName("StockPriceStat").setMaster("local[*]"))

  def stockPriceAvg(lines: RDD[String]) = {
    lines.filter(x => x.split(",").head != "exchange")
        .map(x => (x.split(",")(1), x.split(",")(8).toDouble))
        .mapValues((_,1))
        .reduceByKey((x,y) => (x._1+y._1,x._2+y._2))
        .mapValues(x => x._1 / x._2 )
        .sortBy(_._2,false)
  }

  def stockPriceAvgByStatCounter(lines: RDD[String]) = {
    lines.filter(x => x.split(",").head != "exchange")
        .map(x => (x.split(",")(1), x.split(",")(8).toDouble))
        .groupByKey()
        .mapValues(StatCounter(_).mean)
        .sortBy(_._2,false)
  }

  def stockPriceStDev(lines: RDD[String]) = {
    lines.filter(x => x.split(",").head != "exchange")
        .map(x => (x.split(",")(1), x.split(",")(8).toDouble))
        .groupByKey()
        .mapValues(StatCounter(_).sampleStdev)
        .sortByKey()
  }

  stockPriceAvg(sc.textFile("input//nyse//*")).take(10).foreach(println(_)) //runtime 11s
  //stockPriceAvgByStatCounter(sc.textFile("input//nyse//*")).take(10).foreach(println(_)) //runtime 17s
  //stockPriceStDev(sc.textFile("input//nyse//*")).take(10).foreach(println(_))

  sc.stop()
}
