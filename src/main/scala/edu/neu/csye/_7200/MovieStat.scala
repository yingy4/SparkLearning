package edu.neu.csye._7200

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object MovieStat extends App {

  val sc = new SparkContext(new SparkConf().setAppName("MovieStat").setMaster("local[*]"))

  case class Movie(name: String,avgRating: Double)

  def movieRateAvg(movies: RDD[String], ratings: RDD[String]) = {
    val l = movies.map(x => (x.split("::").head, x.split("::")(1)))
    val r = ratings.map(x => (x.split("::")(1), x.split("::")(2).toDouble))
      l.join(r)
        .map(_._2)
        .mapValues((_,1))
        .reduceByKey((x,y) => (x._1+y._1,x._2+y._2))
        .mapValues(x => x._1 / x._2 )
        .sortBy(_._2,false)
        .map(x => Movie(x._1,x._2))
  }

  movieRateAvg(sc.textFile("input//movies.dat"),sc.textFile("input//ratings.dat")).take(25).foreach(println(_))

  sc.stop()

}
