package de.tuberlin.dima.bdapro.flinkml

/**
  * Created by seema on 16.08.17.
  */
object Config {
  
  var PARENT_DIR: String = ""
  
  def setPath(path: String) {
      PARENT_DIR = path
   }
  
  def pathToClassificationTrainingSet:String =  PARENT_DIR + "/classification.data"

  def pathToRecommendationTrainingSet:String = PARENT_DIR + "/recommendation.data"

  def pathToRegressionTrainingSet:String = PARENT_DIR + "/regression.data"

  def pathToClusteringTrainingSet:String = PARENT_DIR + "/classification.data"

}
