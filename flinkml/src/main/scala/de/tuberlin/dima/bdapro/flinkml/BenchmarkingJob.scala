package de.tuberlin.dima.bdapro.flinkml

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.ExecutionEnvironment
import de.tuberlin.dima.bdapro.flinkml.regression.MLRegression
import de.tuberlin.dima.bdapro.flinkml.clustering.KNearestNeighbors
import de.tuberlin.dima.bdapro.flinkml.classification.SVMClassification
import de.tuberlin.dima.bdapro.flinkml.recommendation.ALSRating
import java.io._
import java.io.IOException;

object BenchmarkingJob {

  def main(args: Array[String]): Unit = {
    
    val results: List[String] = List("FlinkML Accuracy and Execution Time Results \n")
    val start : Long = 0
    val end : Long = 0
    val accuracy : Double = 0
    
    Config.PARENT_DIR = args(0)
    
    val algo = args(1)
    val mode = args(2)
    val test =  Config.PARENT_DIR
    val test2 =  Config.pathToClassificationTrainingSet
    
    System.out.println(test + "-----" + test2)
    val env = ExecutionEnvironment.getExecutionEnvironment
    
    if(algo == "all" | algo == "MLR"){
      val regression = new MLRegression(env)
      print("Hi")
      val MLRAccuracy = regression.execute()
    }
    
//    if(algo == "all" | algo == "KNN"){
//      val clusterling = new KNearestNeighbors(env)
//      val KNNAccuracy = clusterling.execute()
//    }
    
    if(algo == "all" | algo == "SVM"){
      val classification = new SVMClassification(env)
      val SVMAccuracy = classification.execute()
    }
    
    if(algo == "all" | algo == "ALS"){
      val recommendation = new ALSRating(env)
      val ALSAccuracy = recommendation.execute()
    }
    try {
      val file = new File("FlinkMLOutput-" + mode + ".txt")
      val bw = new BufferedWriter(new FileWriter(file))
      for (line <- results) {
        bw.write(line)
      }
      bw.close()
		}
     catch {
         case ex: FileNotFoundException =>{
            println("Missing file exception")
         }
         
         case ex: IOException => {
            println("IO Exception")
         }
     }
    
  }

}
