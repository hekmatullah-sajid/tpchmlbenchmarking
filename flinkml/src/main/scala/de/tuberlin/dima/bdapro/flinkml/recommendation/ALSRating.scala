package de.tuberlin.dima.bdapro.flinkml.recommendation

import de.tuberlin.dima.bdapro.flinkml.Config
import org.apache.flink.api.scala.{DataSet, _}
import org.apache.flink.ml.common.ParameterMap
import org.apache.flink.ml.recommendation.ALS
import org.apache.flink.ml.preprocessing.Splitter

/**
  * Created by seema on 13.08.17.
  */
class ALSRating (val envPassed : ExecutionEnvironment) {
  val env = envPassed

  def execute(): Double = {

    val pathToDataset = Config.pathToRecommendationTrainingSet

    // make parameters available in the web interface
    //env.getConfig.setGlobalJobParameters(params)

    // Read input data set from a csv file

      val inputDS: DataSet[(Int, Int, Double)] = env
        .readCsvFile[(Int, Int, Double)](pathToDataset, ignoreFirstLine = true)
        
      //val trainTestData = Splitter.trainTestSplit(inputDS, 0.8, true)


      // Setup the ALS learner
      val als = ALS()
        .setIterations(10)
        .setNumFactors(10)
        .setBlocks(100)

      // Set the other parameters via a parameter map
      val parameters = ParameterMap()
        .add(ALS.Lambda, 0.9)
        .add(ALS.Seed, 42L)

      // Calculate the factorization
      als.fit(inputDS, parameters)//als.fit(trainTestData.training, parameters)

      // ********* For Testing the model *************** //
      // Read the testing data set from a csv file
      val testingDS: DataSet[(Int, Int,Double)] = inputDS //trainTestData.testing

      // Calculate the ratings according to the matrix factorization
      val predictedRatings = als.predict(testingDS.map(x => (x._1, x._2)))
      //predictedRatings.print()

      val predictionsAndRatings: DataSet[(Double, Double)] =
        testingDS.join(predictedRatings).where(0, 1).equalTo(0, 1) { (l, r) => (l._3, r._3) }
      val count = predictionsAndRatings.count()
      val mse = predictionsAndRatings.collect().map{
        case (rating, prediction) =>
          val err = rating - prediction
          err * err }.sum
          
     var acc =  math.sqrt(mse/count)
     return (acc)


  }
}
