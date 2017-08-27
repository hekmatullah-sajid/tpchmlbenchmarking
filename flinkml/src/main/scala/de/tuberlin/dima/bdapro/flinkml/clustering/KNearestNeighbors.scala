package de.tuberlin.dima.bdapro.flinkml.clustering

import org.apache.flink.api.common.operators.base.CrossOperatorBase.CrossHint
import org.apache.flink.api.scala._
import org.apache.flink.ml.nn.KNN
import org.apache.flink.ml.math.Vector
import org.apache.flink.ml.metrics.distances.SquaredEuclideanDistanceMetric
import org.apache.flink.ml.MLUtils.readLibSVM
import org.apache.flink.ml.common.LabeledVector
import org.apache.flink.ml.RichExecutionEnvironment
import org.apache.flink.ml.preprocessing.Splitter
import de.tuberlin.dima.bdapro.flinkml.Config


class KNearestNeighbors(val envPassed : ExecutionEnvironment) {
  def execute() : Double = {
    val env = envPassed
    val pathToDataset = Config.pathToClusteringTrainingSet
    val dataSet: DataSet[LabeledVector] = env.readLibSVM(pathToDataset)

    val trainTestData = Splitter.trainTestSplit(dataSet, 0.95, true)
    val trainingData: DataSet[Vector] = trainTestData.training.map(lv => lv.vector)
    val testingData: DataSet[Vector] = trainTestData.testing.map(lv => lv.vector)
    testingData.print()
    System.out.println("End of test data")

    val knn = KNN()
      .setK(10)
      .setBlocks(2)
      .setDistanceMetric(SquaredEuclideanDistanceMetric())
      .setUseQuadTree(false)
      .setSizeHint(CrossHint.SECOND_IS_SMALL)

    // run knn join
    knn.fit(trainingData)
    val result = knn.predict(testingData)
    //result.print()
    
    
    
//    val evaluationDS: DataSet[(Double, Double)] = knn.evaluate(testingData, trainTestData.testing.map(lab => lab.label))
//
//    val count = evaluationDS.count()
//    val mse = evaluationDS.collect().map {
//      case (rating, prediction) =>
//        val err = rating - prediction
//        err * err
//    }.sum

//    print(math.sqrt(mse / count))
    
  return 1.0
  }
}