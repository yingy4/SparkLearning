package edu.neu.csye._7200

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object MovieStat extends App {

  case class Movie(name: String,avgRating: Double)

  def movieRateAvg(movies: RDD[String], ratings: RDD[String]) = {
    //Sort by ratings in descending order and title in ascending order
    implicit val myOrdering:Ordering[(Double,String)] = Ordering.Tuple2(Ordering.Double.reverse, Ordering.String)
    val l = movies.map(x => (x.split("::").head, x.split("::")(1)))
    val r = ratings.map(x => (x.split("::")(1), x.split("::")(2).toDouble))
      l.join(r)
        .map(_._2)
        .mapValues((_,1))
        .reduceByKey((x,y) => (x._1+y._1,x._2+y._2))
        .mapValues(x => x._1 / x._2 )
        .sortBy(x => (x._2,x._1))
        .map(Movie.tupled)
  }

  //For Spark 1.0-1.9
  val sc = new SparkContext(new SparkConf().setAppName("MovieStat").setMaster("local[*]"))

  movieRateAvg(sc.textFile("input//movies.dat"),sc.textFile("input//ratings.dat")).take(25).foreach(println(_))

  sc.stop()

  //For Spark 2.0+
  val ss = SparkSession
    .builder()
    .appName("MovieStat")
    .master("local[*]")
    .getOrCreate()

  movieRateAvg(ss.read.textFile("input//movies.dat").rdd,ss.read.textFile("input//ratings.dat").rdd).take(25).foreach(println(_))

  ss.stop()

}
