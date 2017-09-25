package de.tuberlin.dima.bdapro.flinkml

/**
 * <p>
 * The Config object is used to configure the path to directory where data
 * sets are located and define functions to get the intended data set path for executing an algorithm.
 * </p>
 * 
 * @author Hekmatullah Sajid and Seema Narasimha Swamy
 */
object Config {
  
  var PARENT_DIR: String = ""
  
  def setPath(path: String) {
      PARENT_DIR = path
   }
  
  def pathToClassificationTrainingSet:String =  PARENT_DIR + "classification.data"

  def pathToRecommendationTrainingSet:String = PARENT_DIR + "recommendation.data"

  def pathToRegressionTrainingSet:String = PARENT_DIR + "regression.data"

}
