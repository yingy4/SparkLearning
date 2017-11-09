package edu.neu.csye._7200

import org.apache.spark.sql.SparkSession

/**
  * This is an example for running spark application MovieStat in AWS EMR
  * Usage:
  * 1. Use sbt package to generate a jar file and upload it to your AWS S3
  * 2. Create an Cluster in AWS EMR including at least Spark 2.2.0 or higher
  * 3. In your Cluster -> Steps tab, add a new Spark application step
  * 4. Spark-submit options: --class edu.neu.csye._7200.MovieStatAWS
  * 5. Application location: find your jar in your AWS S3
  * 6. Arguments: <input1> <input2> <output>
  *    This example take 3 arguments, input path for movies data, input path for ratings data and output path
  *    Example: s3n://yourbucketname/input/movies_test.dat s3n://yourbucketname/input/ratings_test.dat s3n://yourbucketname/outputfolder
  *    You may find test file under input folder in this repo
  */
object MovieStatAWS {

  def main(args: Array[String]): Unit = {

    val in1 = args.head
    val in2 = args(1)
    val out = args(2)

    implicit val spark = SparkSession
      .builder()
      .appName("MovieStatAWS")
      //.master("local[*]") //Uncomment this line if you want to test in local
      .getOrCreate()

    val movies = MovieStatBySql.createMovieDS(spark.read.textFile(in1))
    val ratings = MovieStatBySql.createRatingsDS(spark.read.textFile(in2))
    val joined = MovieStatBySql.createJoinedDS(movies,ratings,"movieId")
    joined.createTempView("joined")
    joined.cache()

    //repartition(1) will send all data to one node therefore it is not recommended for large dataset, this is only for easy reading.
    spark.sql("select title, avg(rating) as avg from joined group by title order by avg desc").repartition(1).write.mode("append").csv(out)

  }

}
