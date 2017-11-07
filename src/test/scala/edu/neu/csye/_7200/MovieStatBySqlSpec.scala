package edu.neu.csye._7200

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

class MovieStatBySqlSpec extends FlatSpec with Matchers with BeforeAndAfter {

  implicit var spark: SparkSession = _

  before {
    spark = SparkSession
      .builder()
      .appName("MovieStat")
      .master("local[*]")
      .getOrCreate()
  }

  after {
    if (spark != null) {
      spark.stop()
    }
  }

  "movies Dataset" should "work" in {
    val ds = spark.read.textFile("input//movies_test.dat")
    val movies = MovieStatBySql.createMovieDS(ds)
    movies.createTempView("movies")
    movies.cache()
    spark.sql("select count(*) from movies").head().getLong(0) shouldBe 10
  }

  "ratings Dataset" should "work" in {
    val ds = spark.read.textFile("input//ratings_test.dat")
    val ratings = MovieStatBySql.createRatingsDS(ds)
    ratings.createTempView("ratings")
    ratings.cache()
    spark.sql("select count(*) from ratings").head().getLong(0) shouldBe 97265
    spark.sql("select count(distinct movieId) from ratings").head().getLong(0) shouldBe 10
  }

  "joined Dataset" should "work" in {
    val lds = spark.read.textFile("input//movies_test.dat")
    val l = MovieStatBySql.createMovieDS(lds)
    val rds = spark.read.textFile("input//ratings_test.dat")
    val r = MovieStatBySql.createRatingsDS(rds)
    val joined = MovieStatBySql.createJoinedDS(l,r,"movieId")
    joined.createTempView("joined")
    joined.cache()
    spark.sql("select title, avg(rating) as avg from joined group by title order by avg desc").collect().map(_.toString()) shouldBe
      Array("[Toy Story (1995),3.928768573481039]"
      , "[Heat (1995),3.813011098130841]"
      , "[GoldenEye (1995),3.4283012176380185]"
      , "[Sabrina (1995),3.365017361111111]"
      , "[Jumanji (1995),3.208070146276596]"
      , "[Grumpier Old Men (1995),3.150385109114249]"
      , "[Tom and Huck (1995),3.131256952169077]"
      , "[Father of the Bride Part II (1995),3.0774351786965664]"
      , "[Sudden Death (1995),2.9968228752978554]"
      , "[Waiting to Exhale (1995),2.860544217687075]")
  }

}