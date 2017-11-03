package edu.neu.csye._7200

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

class WordCountSpark2Spec extends FlatSpec with Matchers with BeforeAndAfter  {

  private var ss: SparkSession = _

  before {
    ss = SparkSession
      .builder()
      .appName("WordCount")
      .master("local[*]")
      .getOrCreate()
  }

  after {
    if (ss != null) {
      ss.stop()
    }
  }

  "result" should "right for wordCount" in {
    WordCount.wordCount(ss.read.textFile("input//WordCount.txt").rdd," ").collect() should matchPattern {
      case Array(("Hello",3),("World",3),("Hi",1)) =>
    }
  }
}
