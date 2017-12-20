package edu.neu.csye._7200

import org.scalatest.{FlatSpec, Matchers}

class CountLabelsSpec extends FlatSpec with Matchers{

  behavior of "checkStartWith"

  it should "work for true" in {
    CountLabels.checkStartWith("abc","abc:0.5") shouldBe (true, "abc:0.5")
  }

  it should "work for false" in {
    CountLabels.checkStartWith("abc","cba:0.5") shouldBe (false, "cba:0.5")
  }

  behavior of "implicit conversion to Double"

  it should "work for true and Double" in {
    import CountLabels._
    (CountLabels.checkStartWith("abc","abc:0.5"):Double) shouldBe 0.5
  }

  it should "work for false and Double" in {
    import CountLabels._
    (CountLabels.checkStartWith("abc","cba:0.5"):Double) shouldBe 0.0
  }

  behavior of "implicit conversion to Int"

  it should "work for true and Int" in {
    import CountLabels._
    (CountLabels.checkStartWith("abc","abc:0.5"):Int) shouldBe 1
  }

  it should "work for false and Int" in {
    import CountLabels._
    (CountLabels.checkStartWith("abc","cba:0.5"):Int) shouldBe 0
  }

  behavior of "markLabels"

  it should "work for label in first place" in {
    CountLabels.markLabels(Seq("abc","abc:0.5","xxx:0.1","xxx:0.1","xxx:0.1","xxx:0.1")) shouldBe ("abc",0.5,0,0,0,0)
  }

  it should "work for label not in first place" in {
    CountLabels.markLabels(Seq("abc","xxx:0.5","abc:0.1","xxx:0.1","xxx:0.1","xxx:0.1")) shouldBe ("abc",0.0,1,0,0,0)
  }

  behavior of "checkZeroAndCount"

  it should "work for positive case" in {
    CountLabels.checkZeroAndCount(0.88) shouldBe (0.88,1)
  }

  it should "work for negative case" in {
    CountLabels.checkZeroAndCount(0.0) shouldBe (0.0,0)
  }

  behavior of "plusForTwo"

  it should "work" in {
    CountLabels.plusForTwo((0.1,1),(0.2,3))._1 shouldBe 0.3 +- 1E-10
    CountLabels.plusForTwo((0.1,1),(0.2,3))._2 shouldBe 4
  }

  behavior of "plusForFour"

  it should "work" in {
    CountLabels.plusForFour((0.1,0.2,0.3,1),(0.2,0.3,0.4,1))._1 shouldBe 0.3 +- 1E-10
    CountLabels.plusForFour((0.1,0.2,0.3,1),(0.2,0.3,0.4,1))._2 shouldBe 0.5 +- 1E-10
    CountLabels.plusForFour((0.1,0.2,0.3,1),(0.2,0.3,0.4,1))._3 shouldBe 0.7 +- 1E-10
    CountLabels.plusForFour((0.1,0.2,0.3,1),(0.2,0.3,0.4,1))._4 shouldBe 2
  }


}
