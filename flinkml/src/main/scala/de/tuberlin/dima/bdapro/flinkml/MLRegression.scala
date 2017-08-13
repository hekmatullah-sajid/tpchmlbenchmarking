package de.tuberlin.dima.bdapro.flinkml

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.ml.RichExecutionEnvironment
import org.apache.flink.ml.MLUtils
import org.apache.flink.ml.common.LabeledVector
import org.apache.flink.ml.preprocessing.Splitter
import org.apache.flink.ml.preprocessing.Splitter.TrainTestDataSet
import org.apache.flink.ml.regression.MultipleLinearRegression

object MLRegression {
  def main(args:Array[String]) ={
    
    print("Hello World")
     val env = ExecutionEnvironment.getExecutionEnvironment
     
    val dataSet: DataSet[LabeledVector] = MLUtils.readLibSVM(env, "/media/sf_VM-SharedFoler/100lines.txt")
    val dataMultiRandom: Array[DataSet[LabeledVector]] = Splitter.multiRandomSplit(dataSet, Array(0.8, 0.2))
   
    val trainingData: DataSet[LabeledVector] = dataMultiRandom(0)
    val testingData: DataSet[Vector] = dataMultiRandom(1).map(lv => lv.vector)
    
    val mlr = MultipleLinearRegression()
      .setStepsize(1.0)
      .setIterations(100)
      .setConvergenceThreshold(0.001)
    
    mlr.fit(trainingData)
    
    // The fitted model can now be used to make predictions
    val predictions = mlr.predict(testingData)
        print("Hello World")
  }
}