package de.tuberlin.dima.bdapro.flinkml.classification

import de.tuberlin.dima.bdapro.flinkml.Config
import org.apache.flink.api.scala._
import org.apache.flink.ml.RichExecutionEnvironment
import org.apache.flink.ml.classification.SVM
import org.apache.flink.ml.common.LabeledVector
import org.apache.flink.ml.math.Vector
import org.apache.flink.ml.preprocessing.Splitter
import org.apache.flink.api.scala.{DataSet, _}

/**
  * Created by seema on 13.08.17.
  */
class SVMClassification(val envPassed : ExecutionEnvironment) {
  val env = envPassed

  def execute() : Double = {

//    val params: ParameterTool = ParameterTool.fromArgs(args)
//    val env = ExecutionEnvironment.getExecutionEnvironment
//    env.getConfig.setGlobalJobParameters(params)

    val pathToDataset = Config.pathToClassificationTrainingSet
    // Read the training data set, from a LibSVM formatted file
    val inputds: DataSet[LabeledVector] = env.readLibSVM(pathToDataset)

    val trainTestData = Splitter.trainTestSplit(inputds, 0.7, true)
    val trainingData: DataSet[LabeledVector] = trainTestData.training
    val testingData: DataSet[LabeledVector] = trainTestData.testing

    // Create the SVM learner
    val svm = SVM()
      .setBlocks(20).setOutputDecisionFunction(false)

    // Learn the SVM model
    svm.fit(trainingData)
    // Read the testing data set
    //val testingDS: DataSet[LabeledVector] = env.readLibSVM(pathToDataset)
    val evaluationDS: DataSet[(Double, Double)] = svm.evaluate(testingData.map(x => (x.vector, x.label)))

    val squaredError: DataSet[Double] = evaluationDS.map((tuple: (Double, Double)) => (if (tuple._1 == tuple._2) 1.0 else 0.0, 1))
      .reduce((tuple: (Double, Int), tuple0: (Double, Int)) => (tuple._1 + tuple0._1, tuple._2 + tuple0._2))
      .map(tuple => (math.sqrt(tuple._1 / tuple._2)))

    return squaredError.collect().last

  }

}
