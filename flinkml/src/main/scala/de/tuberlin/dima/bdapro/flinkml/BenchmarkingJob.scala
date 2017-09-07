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
    
    Config.PARENT_DIR = args(0)
      
    val algo = args(1)
    val mode = args(2)   
    val env = ExecutionEnvironment.getExecutionEnvironment
    
    var start : Long = 0
    var end : Long = 0
    var exeTime: Long = 0
    var accuracy : Double = 0
    
    try {
      val file = new File("FlinkMLOutput-" + mode + ".txt")
      val bw = new FileWriter(file, true)
      bw.write("FlinkML Accuracy and Execution Time Results \n")
//      if(algo == "all" | algo == "MLR"){
//        start = System.currentTimeMillis()
//        val regression = new MLRegression(env)
//        val MLRAccuracy = regression.execute()
//        end = System.currentTimeMillis()
//        exeTime = (end - start)
//        bw.write("MultipleLinearRegression, " + ", " + exeTime + ", " + MLRAccuracy + "\n")
//      }
      
      if(algo == "all" | algo == "KNN"){
          start = System.currentTimeMillis()
        val clusterling = new KNearestNeighbors(env)
        val KNNAccuracy = clusterling.execute()
        end = System.currentTimeMillis()
        exeTime = (end - start)
        bw.write("KNearestNeighbors, " + ", " + exeTime + ", " + KNNAccuracy + "\n")
      }
      
//      if(algo == "all" | algo == "SVM"){
//        start = System.currentTimeMillis()
//        val classification = new SVMClassification(env)
//        val SVMAccuracy = classification.execute()
//        end = System.currentTimeMillis()
//        exeTime = (end - start)
//        bw.write("SVMClassification, "  + exeTime + ", " + SVMAccuracy + "\n")
//      }
//      
//      if(algo == "all" | algo == "ALS"){
//        start = System.currentTimeMillis()
//        val recommendation = new ALSRating(env)
//        val ALSAccuracy = recommendation.execute()
//        end = System.currentTimeMillis()
//        exeTime = (end - start)
//         bw.write("ALSRating, " + exeTime + ", " + ALSAccuracy + "\n")
//      }
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
