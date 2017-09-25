package de.tuberlin.dima.bdapro.flinkml.regression

import org.apache.flink.api.java.io.CsvOutputFormat
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

/**
 * Class for testing the  Multiple Linear Regression ML algorithm.
 * 
 * @author Hekmatullah Sajid
 *
 */
class MLRegression(val envPassed: ExecutionEnvironment) {
  
  	/**
     * 
     * The execute method is used to test the algorithm.
     * The input data set is in libsvm format which is split into two parts 80% for learning and the rest for testing.
     * The method returns "Root Mean Squared Error (RMSE)" for the algorithm.
     * 
     */
  def execute(): Double = {

    val env = envPassed

    /*
     * Load the data stored in LIBSVM format as a DataFrame.
     */
    val pathToDataset = Config.pathToRegressionTrainingSet
    val dataSet: DataSet[LabeledVector] = env.readLibSVM(pathToDataset)

    /*
		 * Split the data into training and test sets (80% training and 20% held for testing).
		 */
    val trainTestData = Splitter.trainTestSplit(dataSet, 0.8, true)
    val trainingData: DataSet[LabeledVector] = trainTestData.training
    val testingData: DataSet[Vector] = trainTestData.testing.map(lv => lv.vector)
    
    
    /*
		 * Train a MultipleLinearRegression model.
		 * The number of iterations is set to 20, this can be increased to get a lower test error.
		 */
    val mlr = MultipleLinearRegression()
      .setStepsize(1.0)
      .setIterations(20)
      .setConvergenceThreshold(0.001)

    /*
     * Fit the model.
     */
    mlr.fit(trainingData)

    /*
     * The fitted model can now be used to make predictions
     */
    val predictions: DataSet[(Vector, Double)] = mlr.predict(testingData)


    /*
     * Evaluate prediction and compute test error.
     */
    val evaluationDS: DataSet[(Double, Double)] = mlr.evaluate(trainTestData.testing.map(x => (x.vector, x.label)))

    val rmse = evaluationDS.map((tuple: (Double, Double)) => ((tuple._1 - tuple._2) * (tuple._1 - tuple._2), 1))
      .reduce((tuple: (Double, Int), tuple0: (Double, Int)) => (tuple._1 + tuple0._1, tuple._2 + tuple0._2))
      .map(tuple => (math.sqrt(tuple._1 / tuple._2)))

    return rmse.collect().last
  }
}
