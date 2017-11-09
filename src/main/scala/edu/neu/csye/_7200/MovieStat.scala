package edu.neu.csye._7200

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object MovieStat extends App {

  case class Movie(name: String,avgRating: Double)

  object Movie extends ((String,Double) => Movie) {

    object OrderingMovieAvgRating extends Ordering[Movie] {
      def compare(x: Movie, y: Movie): Int = x.avgRating.compare(y.avgRating)
    }

    object OrderingMovieName extends Ordering[Movie] {
      def compare(x: Movie, y: Movie): Int = x.name.compare(y.name)
    }

  }

  def movieRateAvg(movies: RDD[String], ratings: RDD[String]) = {
    //Sort by ratings in descending order and title in ascending order
    val comparerAvgRating:Comparer[Movie] = Movie.OrderingMovieAvgRating
    val comparerName:Comparer[Movie] = Movie.OrderingMovieName
    implicit val movieOrdering:Ordering[Movie] = (comparerAvgRating.invert orElse comparerName).toOrdering
    val l = movies.map(x => (x.split("::").head, x.split("::")(1)))
    val r = ratings.map(x => (x.split("::")(1), x.split("::")(2).toDouble))
    l.join(r)
        .map(_._2)
        .mapValues((_,1))
        .reduceByKey((x,y) => (x._1+y._1,x._2+y._2))
        .mapValues(x => x._1 / x._2 )
        .map(Movie.tupled)
        .sortBy(x => x)
  }

  //This method filter the ratings rdd which has the movieId that appears in movies rdd
  def filterRatings(movies: RDD[String], ratings: RDD[String]) = {
    val l = movies.map(x => (x.split("::").head, 0))
    val r = ratings.map(x => (x.split("::")(1), x))
    l.join(r)
        .map(_._2._2)
  }

  //For Spark 1.0-1.9
  val sc = new SparkContext(new SparkConf().setAppName("MovieStat").setMaster("local[*]"))

  movieRateAvg(sc.textFile("input//movies.dat"),sc.textFile("input//ratings.dat")).take(25).foreach(println(_))

  sc.stop()

  //For Spark 2.0+
  val spark = SparkSession
    .builder()
    .appName("MovieStat")
    .master("local[*]")
    .getOrCreate()

  movieRateAvg(spark.read.textFile("input//movies.dat").rdd,spark.read.textFile("input//ratings.dat").rdd).take(25).foreach(println(_))

  //filterRatings(spark.read.textFile("input//movies_test.dat").rdd,spark.read.textFile("input//ratings.dat").rdd).coalesce(1).saveAsTextFile("input//ratings_test")

  spark.stop()

}
