package edu.neu.csye._7200

import edu.neu.csye._7200.CountLabels.{labelCount, totalStat}
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

class CountLabelsSparkSpec extends FlatSpec with Matchers with BeforeAndAfter {

  var spark: SparkSession = _

  before {
    spark = SparkSession
      .builder()
      .appName("MovieStatSpark2")
      .master("local[*]")
      .getOrCreate()
  }

  after {
    if (spark != null) {
      spark.stop()

    }
  }

  "labelCount" should "work for test input" in {
    val lc = labelCount(spark.read.textFile("input//CountLabelsTest.txt").rdd,",")
    lc.collect() shouldBe Array(("caesar_salad",0.7,0.8499234628571427,1.0),("chicken_curry",0.7,0.7618182714285713,1.0))
  }

  "totalStat" should "work for test input" in {
    val lc = labelCount(spark.read.textFile("input//CountLabelsTest.txt").rdd,",")
    totalStat(lc).collect() shouldBe Array(("total",0.7,0.8058708671428569,1.0))
  }

}
