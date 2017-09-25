package de.tuberlin.dima.bdapro.flinkml.recommendation

import breeze.linalg.Axis._1
import de.tuberlin.dima.bdapro.flinkml.Config
import org.apache.flink.api.scala.{DataSet, _}
import org.apache.flink.ml.common.{LabeledVector, ParameterMap}
import org.apache.flink.ml.math.Vector
import org.apache.flink.ml.recommendation.ALS
import org.apache.flink.ml.preprocessing.Splitter

/**
 * Class for testing the ALS Rating (Recommendation) ML algorithm
 * 
 * @author Seema Narasimha Swamy
 *
 */
class ALSRating (val envPassed : ExecutionEnvironment) {
  val env = envPassed

  /**
   * 
   * The execute method is used to test the algorithm.
   * The input data set (movielens) is in CSV format which is split into two parts 80% for learning and the rest for testing.
   * The method returns "Root Mean Square Error" for the algorithm.
   * 
   */
  def execute(): Double = {

    val pathToDataset = Config.pathToRecommendationTrainingSet
    
      /**
       * Read input data set from a csv file
       */

      val inputDS: DataSet[(Int, Int, Double)] = env
        .readCsvFile[(Int, Int, Double)](pathToDataset, ignoreFirstLine = true)

      /*
       * Setup the ALS learner.
       */
      val als = ALS()
        .setIterations(10)
        .setNumFactors(10)
        .setBlocks(100)

      /*
       * Set the other parameters via a parameter map
       */
      val parameters = ParameterMap()
        .add(ALS.Lambda, 0.9)
        .add(ALS.Seed, 42L)
  
      /*
  		 * Split the data into training and test sets (80% training and 20% held for testing).
  		 */
      val trainTestData = Splitter.trainTestSplit(inputDS, 0.8, true)
      val trainingData: DataSet[(Int, Int, Double)] = trainTestData.training
      val testingData: DataSet[(Int, Int, Double)] = trainTestData.testing
      
      /*
       * Fit the model.
       */
      als.fit(trainingData, parameters)

      // ********* For Testing the model *************** //

      /*
       * Calculate the ratings according to the matrix factorization
       * Evaluate the algorithm and find the test error.
       */
      val predictedRatings = als.predict(testingData.map(x => (x._1, x._2)))
      
      var foo = 0.9
      val predictionsAndRatings: DataSet[(Double, Double)] =
          testingData.join(predictedRatings).where(0, 1).equalTo(0, 1) { (l, r) => (l._3, r._3) }
  
      val squaredError: DataSet[Double] = predictionsAndRatings.map((tuple: (Double, Double)) => ((tuple._1 - tuple._2) * (tuple._1 - tuple._2), 1))
        .reduce((tuple: (Double, Int), tuple0: (Double, Int)) => (tuple._1 + tuple0._1, tuple._2 + tuple0._2))
        .map(tuple => (math.sqrt(tuple._1 / tuple._2)))
      val result = squaredError.collect()
      
      return result.last

  }
}
