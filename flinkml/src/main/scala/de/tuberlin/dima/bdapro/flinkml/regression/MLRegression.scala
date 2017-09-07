package de.tuberlin.dima.bdapro.flinkml.regression

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.ml.RichExecutionEnvironment
//import org.apache.flink.ml.MLUtils._
import org.apache.flink.ml.MLUtils.readLibSVM
import org.apache.flink.ml.common.LabeledVector
import org.apache.flink.ml.preprocessing.Splitter
import org.apache.flink.ml.regression.MultipleLinearRegression
import org.apache.flink.ml.preprocessing.Splitter.TrainTestDataSet
import de.tuberlin.dima.bdapro.flinkml.Config

import org.apache.flink.ml.math.Vector

class MLRegression(val envPassed: ExecutionEnvironment) {
  def execute(): Double = {

    val env = envPassed
    val pathToDataset = Config.pathToRegressionTrainingSet
    val dataSet: DataSet[LabeledVector] = env.readLibSVM(pathToDataset)

    val trainTestData = Splitter.trainTestSplit(dataSet, 0.8, true)
    val trainingData: DataSet[LabeledVector] = trainTestData.training
    val testingData: DataSet[Vector] = trainTestData.testing.map(lv => lv.vector)

    //    val dataMultiRandom: Array[DataSet[LabeledVector]] = Splitter.multiRandomSplit(dataSet, Array(0.8, 0.2))
    //    val trainingData: DataSet[LabeledVector] = dataMultiRandom(0)
    //    val testingData: DataSet[LabeledVector] = dataMultiRandom(1)

    //  val testingData: DataSet[Vector] = dataMultiRandom(1).map(lv => lv.vector)

    val mlr = MultipleLinearRegression()
      .setStepsize(1.0)
      .setIterations(100)
      .setConvergenceThreshold(0.001)

    mlr.fit(trainingData)


    // The fitted model can now be used to make predictions
    //  val predictions: DataSet[(Vector, Double)] = mlr.predict(testingData)


    val evaluationDS: DataSet[(Double, Double)] = mlr.evaluate(trainTestData.testing.map(x => (x.vector, x.label)))

//    val count = evaluationDS.count()
//    val mse = evaluationDS.collect().map {
//      case (label, prediction) =>
//        val err = label - prediction
//        err * err
//    }.sum
//    val accuracy = math.sqrt(mse / count)
//    return accuracy
    return 10

  }
}
