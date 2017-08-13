package scala.de.tuberlin.dima.bdapro.flinkml.classification

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.ml.RichExecutionEnvironment
import org.apache.flink.ml.classification.SVM
import org.apache.flink.ml.common.LabeledVector

/**
  * Created by seema on 13.08.17.
  */
object SVMClassification {
  def main(args: Array[String]): Unit = {

    val params: ParameterTool = ParameterTool.fromArgs(args)
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.getConfig.setGlobalJobParameters(params)

    val pathToTrainingFile = params.getRequired("training")
    val pathToTestingFile = params.get("testing")

    // Read the training data set, from a LibSVM formatted file
    val trainingDS: DataSet[LabeledVector] = env.readLibSVM(pathToTrainingFile)

    // Create the SVM learner
    val svm = SVM()
      .setBlocks(10)

    // Learn the SVM model
    svm.fit(trainingDS)
    // Read the testing data set
    val testingDS: DataSet[LabeledVector] = env.readLibSVM(pathToTestingFile)
    val evaluationDS: DataSet[(Double, Double)] = svm.evaluate(testingDS.map(x => (x.vector, x.label)))
    evaluationDS.print()
    env.execute("SVM Classification")

//    val predictionPairs = svm.evaluate(test)
//
//    val absoluteErrorSum = predictionPairs.collect().map{
//      case (truth, prediction) => Math.abs(truth - prediction)}.sum
//
//    absoluteErrorSum should be < 15.0
//def calculateAccuracy(predictions: RDD[(Double, Double)], numExamples: Long): Double = {
//  predictions.map{case (pred, label) =>
//    if (pred == label) 1.0 else 0.0
//  }.sum() * 100.0 / numExamples
  }

}
