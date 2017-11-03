package edu.neu.csye._7200

import edu.neu.csye._7200.MovieStat.Movie
import org.apache.spark.sql.{Encoders, SparkSession}
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

class MovieStatSpark2Spec extends FlatSpec with Matchers with BeforeAndAfter {

  var ss: SparkSession = _

  before {
    ss = SparkSession
      .builder()
      .appName("MovieStat")
      .master("local[*]")
      .getOrCreate()
  }

  after {
    if (ss != null) {
      ss.stop()
    }
  }

  "result" should "right for MovieRateAvg" in {
    val sparkSession = SparkSession.getActiveSession.getOrElse(ss)
    import sparkSession.implicits._
    val movies = Array("1::Toy Story (1995)::Adventure|Animation|Children|Comedy|Fantasy", "2::Jumanji (1995)::Adventure|Children|Fantasy", "3::Grumpier Old Men (1995)::Comedy|Romance")
    val ratings = Array("1::1::5::838985046", "1::1::4::838985046", "1::1::4::838985046", "1::2::4::838985046", "1::3::4::838985046")
    MovieStat.movieRateAvg(ss.createDataset(movies).rdd,ss.createDataset(ratings).rdd).collect() should matchPattern {
      case Array(Movie("Toy Story (1995)",4.333333333333333), Movie("Grumpier Old Men (1995)",4.0), Movie("Jumanji (1995)",4.0)) =>
    }
  }

}
