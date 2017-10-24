package edu.neu.csye._7200

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

class StockPriceStatSpec extends FlatSpec with Matchers with BeforeAndAfter {

  private var sc: SparkContext = _

  before {
    sc = new SparkContext(new SparkConf().setAppName("StockPriceStat").setMaster("local[*]"))
  }

  after {
    if (sc != null) {
      sc.stop()
    }
  }

  "result" should "right for stockPriceAvg" in {
    StockPriceStat.stockPriceAvg(sc.textFile("input//nyse//*Q.csv")).collect() should matchPattern {
      case Array(("QRR",11.067500000000006), ("QXM",5.350100286532951), ("QTM",5.051219696969693)) =>
    }
  }

  "result" should "right for stockPriceAvgByStatCounter" in {
    StockPriceStat.stockPriceAvgByStatCounter(sc.textFile("input//nyse//*Q.csv")).collect() should matchPattern {
      case Array(("QRR",11.06749999999999), ("QXM",5.350100286532955), ("QTM",5.0512196969696985)) =>
    }
  }

  "result" should "right for stockPriceStDev" in {
    StockPriceStat.stockPriceStDev(sc.textFile("input//nyse//*Q.csv")).collect() should matchPattern {
      case Array(("QRR",2.2623154802207286), ("QTM",4.427839742682631), ("QXM",2.8237055776681648)) =>
    }
  }
}
