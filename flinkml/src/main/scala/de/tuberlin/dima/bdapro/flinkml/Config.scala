package de.tuberlin.dima.bdapro.flinkml

/**
  * Created by seema on 16.08.17.
  */
object Config {
  def pathToClassificationTrainingSet:String = "/Users/seema/IdeaProjects/tpchml/tpchmlbenchmarking/dataset_ml" + "/classification/leu"
  def pathToClassificationTestingSet:String = "/Users/seema/IdeaProjects/tpchml/tpchmlbenchmarking/dataset_ml" + "/classification/leu.t"

  def pathToRecommendationTrainingSet:String = "/Users/seema/IdeaProjects/tpchml/tpchmlbenchmarking/dataset_ml" + "/recommendation/ml-latest-small/ratings.csv"
  def pathToRecommendationTestingSet:String = "/Users/seema/IdeaProjects/tpchml/tpchmlbenchmarking/dataset_ml" + "/recommendation/ml-latest-small/ratings.csv"

  def pathToRegressionTrainingSet:String = "/Users/seema/IdeaProjects/tpchml/tpchmlbenchmarking/dataset_ml" + "/regression/mpg.txt"
  def pathToRegressionTestingSet:String = "/Users/seema/IdeaProjects/tpchml/tpchmlbenchmarking/dataset_ml" + "/regression/mpg.txt"

  def pathToClusteringTrainingSet:String = "/Users/seema/IdeaProjects/tpchml/tpchmlbenchmarking/dataset_ml" + "/clustering/sample_kmeans_data.txt"
  def pathToClusteringTestingSet:String = "/Users/seema/IdeaProjects/tpchml/tpchmlbenchmarking/dataset_ml" + "/clustering/sample_kmeans_data.txt"

}
