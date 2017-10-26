package edu.neu.csye._7200

import edu.neu.csye._7200.MovieStat.Movie
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

class MovieStatSpec extends FlatSpec with Matchers with BeforeAndAfter {

  private var sc: SparkContext = _

  before {
    sc = new SparkContext(new SparkConf().setAppName("MovieStat").setMaster("local[*]"))
  }

  after {
    if (sc != null) {
      sc.stop()
    }
  }

  "result" should "right for MovieRateAvg" in {
    val movies = Array("1::Toy Story (1995)::Adventure|Animation|Children|Comedy|Fantasy", "2::Jumanji (1995)::Adventure|Children|Fantasy")
    val ratings = Array("1::1::5::838985046", "1::1::4::838985046", "1::1::4::838985046", "1::2::4::838985046")
    MovieStat.movieRateAvg(sc.parallelize(movies),sc.parallelize(ratings)).collect() should matchPattern {
      case Array(Movie("Toy Story (1995)",4.333333333333333), Movie("Jumanji (1995)",4.0)) =>
    }
  }

}
