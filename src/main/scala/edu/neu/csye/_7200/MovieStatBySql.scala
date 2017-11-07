package edu.neu.csye._7200

import org.apache.spark.sql.{Dataset, SparkSession}

object MovieStatBySql extends App {

  case class Movies(movieId: Int, title: String, movieType: String)

  case class Ratings(userId: Int, movieId: Int, rating: Double, time: Long)

  case class MovieAvg(title: String, rating: Double)

  implicit val spark = SparkSession
    .builder()
    .appName("MovieStatBySql")
    .master("local[*]")
    .getOrCreate()

  def createMovieDS(ds:Dataset[String])(implicit spark:SparkSession) = {
    import spark.implicits._
    ds.map(x => x.split("::"))
      .map(x => (x.head.toInt,x(1),x(2)))
      .map(Movies.tupled)
      .as[Movies]
  }

  val moviesDS = createMovieDS(spark.read.textFile("input//movies.dat"))

  def createRatingsDS(ds:Dataset[String])(implicit spark:SparkSession) = {
    import spark.implicits._
    ds.map(x => x.split("::"))
      .map(x => (x.head.toInt,x(1).toInt,x(2).toDouble,x(3).toLong))
      .map(Ratings.tupled)
      .as[Ratings]
  }

  val ratingsDS = createRatingsDS(spark.read.textFile("input//ratings.dat"))

  def createJoinedDS(l:Dataset[Movies],r:Dataset[Ratings],joinKey:String) = l.join(r,joinKey)

  val joined = createJoinedDS(moviesDS,ratingsDS,"movieId")

  joined.createTempView("joined")

  joined.cache()

  spark.sql("select title, avg(rating) as rating from joined group by title order by avg desc").show(10)

  spark.stop()

}
