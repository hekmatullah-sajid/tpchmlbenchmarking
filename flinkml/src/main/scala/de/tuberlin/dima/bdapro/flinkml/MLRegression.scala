package de.tuberlin.dima.bdapro.flinkml

import org.apache.flink.api.scala.DataSet
import org.apache.flink.ml.MLUtils
import org.apache.flink.ml.common.LabeledVector
import org.apache.flink.ml.preprocessing.Splitter
import org.apache.flink.ml.preprocessing.Splitter.TrainTestDataSet
import org.apache.flink.ml.regression.MultipleLinearRegression

object MLRegression {
  def main(args:Array[String]) ={
    
    print("Hello World")
      
    val dataSet: DataSet[LabeledVector] = MLUtils.readLibSVM(env, "/path/to/svmguide1")
    val trainTestData: DataSet[TrainTestDataSet] = Splitter.trainTestSplit(dataSet)
    val trainingData: DataSet[LabeledVector] = trainTestData.training
    val testingData: DataSet[Vector] = trainTestData.testing.map(lv => lv.vector)
    
    val mlr = MultipleLinearRegression()
      .setStepsize(1.0)
      .setIterations(100)
      .setConvergenceThreshold(0.001)
    
    mlr.fit(trainingData)
    
    // The fitted model can now be used to make predictions
    val predictions: DataSet[LabeledVector] = mlr.predict(testingData)
  }
}