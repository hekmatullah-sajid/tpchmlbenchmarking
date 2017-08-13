package de.tuberlin.dima.bdapro.flinkml

<<<<<<< HEAD
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.ml.MLUtils._
import org.apache.flink.ml.MLUtils
import org.apache.flink.api.scala._
import org.apache.flink.ml.regression.MultipleLinearRegression
import org.apache.flink.ml.common.LabeledVector
import org.apache.flink.ml.preprocessing.Splitter
import org.apache.flink.ml.math.Vector 
import org.apache.flink.api.scala._
=======
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.ml.RichExecutionEnvironment
import org.apache.flink.ml.MLUtils
import org.apache.flink.ml.common.LabeledVector
import org.apache.flink.ml.preprocessing.Splitter
import org.apache.flink.ml.preprocessing.Splitter.TrainTestDataSet
import org.apache.flink.ml.regression.MultipleLinearRegression
>>>>>>> fbf7ad2ba7717b80b6ad67be4ae6e931dcd7bfa0

object MLRegression {
  def main(args:Array[String]) ={
    
    
    print("Hello World")
<<<<<<< HEAD
     val env = ExecutionEnvironment.getExecutionEnvironment
     
    val dataSet: DataSet[LabeledVector] = MLUtils.readLibSVM(env, "/media/sf_VM-SharedFoler/100lines.txt")
    val dataMultiRandom: Array[DataSet[LabeledVector]] = Splitter.multiRandomSplit(dataSet, Array(0.8, 0.2))
   
    val trainingData: DataSet[LabeledVector] = dataMultiRandom(0)
    val testingData: DataSet[Vector] = dataMultiRandom(1).map(lv => lv.vector)
=======

    val params: ParameterTool = ParameterTool.fromArgs(args)
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.getConfig.setGlobalJobParameters(params)

    val pathToTrainingFile = params.getRequired("training")
    val pathToTestingFile = params.get("testing")

    // Read the training data set, from a LibSVM formatted file
    val trainingDS: DataSet[LabeledVector] = env.readLibSVM(pathToTrainingFile)
      
    val dataSet: DataSet[LabeledVector] = MLUtils.readLibSVM(env, "/path/to/svmguide1")
    val trainTestData: DataSet[TrainTestDataSet] = Splitter.trainTestSplit(dataSet)
    val trainingData: DataSet[LabeledVector] = trainTestData.training
    val testingData: DataSet[Vector] = trainTestData.testing.map(lv => lv.vector)
>>>>>>> fbf7ad2ba7717b80b6ad67be4ae6e931dcd7bfa0
    
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