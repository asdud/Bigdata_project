package day0713

import org.apache.spark.mllib.recommendation.ALS

object ALSDemo {
  def main(args: Array[String]): Unit = {
    ALS.train(ratings, rank, iterations, lambda)
  }
}