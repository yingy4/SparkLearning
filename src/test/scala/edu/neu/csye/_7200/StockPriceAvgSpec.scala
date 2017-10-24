package edu.neu.csye._7200

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

class StockPriceAvgSpec extends FlatSpec with Matchers with BeforeAndAfter {

  private var sc: SparkContext = _

  before {
    sc = new SparkContext(new SparkConf().setAppName("StockPriceAvg").setMaster("local[*]"))
  }

  after {
    if (sc != null) {
      sc.stop()
    }
  }

  "result" should "right for stockPriceAvg" in {
    StockPriceAvg.stockPriceAvg(sc.textFile("input//nyse//*Q.csv")).collect() should matchPattern {
      case Array(("QRR",11.067500000000006), ("QXM",5.350100286532951), ("QTM",5.051219696969693)) =>
    }
  }
}
