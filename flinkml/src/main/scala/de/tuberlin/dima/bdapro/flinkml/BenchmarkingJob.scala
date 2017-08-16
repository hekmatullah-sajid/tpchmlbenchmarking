package de.tuberlin.dima.bdapro.flinkml

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.ExecutionEnvironment

import scala.de.tuberlin.dima.bdapro.flinkml.classification.SVMClassification
import scala.de.tuberlin.dima.bdapro.flinkml.recommendation.ALSRating

/**
  * Created by seema on 16.08.17.
  */
object BenchmarkingJob {

  def main(args: Array[String]): Unit = {

    val params: ParameterTool = ParameterTool.fromArgs(args)
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.getConfig.setGlobalJobParameters(params)

    val svmClassification = new SVMClassification(env)
    svmClassification.execute()

    val alsRating = new ALSRating(env)
    alsRating.execute()
  }

}
