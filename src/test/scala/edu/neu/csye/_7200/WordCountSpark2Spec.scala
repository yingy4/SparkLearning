package edu.neu.csye._7200

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

class WordCountSpark2Spec extends FlatSpec with Matchers with BeforeAndAfter  {

  private var spark: SparkSession = _

  before {
    spark = SparkSession
      .builder()
      .appName("WordCount")
      .master("local[*]")
      .getOrCreate()
  }

  after {
    if (spark != null) {
      spark.stop()
    }
  }

  "result" should "right for wordCount" in {
    WordCount.wordCount(spark.read.textFile("input//WordCount.txt").rdd," ").collect() should matchPattern {
      case Array(("Hello",3),("World",3),("Hi",1)) =>
    }
  }
}
