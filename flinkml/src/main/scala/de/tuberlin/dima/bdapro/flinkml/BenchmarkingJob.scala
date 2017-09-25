package de.tuberlin.dima.bdapro.flinkml

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.ExecutionEnvironment
import de.tuberlin.dima.bdapro.flinkml.regression.MLRegression
import de.tuberlin.dima.bdapro.flinkml.classification.SVMClassification
import de.tuberlin.dima.bdapro.flinkml.recommendation.ALSRating
import java.io._
import java.io.IOException;


/**
 * Main class of the project for testing ML Algorithms on Apache Flink. How
 * to run the jar on Flink: ./bin/flink run path-to-jar path-to-datasets algorithm/all Mode(Cluster/Local)
 * 
 * @author Hekmatullah Sajid and Seema Narasimha Swamy
 *
 */
object BenchmarkingJob {

  def main(args: Array[String]): Unit = {
    
    /**
		 * ARGUMENT PARSING The main method expects minimum three arguments. 
		 * First argument should be the path to directory where data sets are located.
		 * Second argument should be the all or abbreviation of the algorithm to be executed.
		 * Third argument should be the execution mode (Cluster/Local)
		 */
    if (args.length <= 0 || args.length > 3) {
			throw new IllegalArgumentException(
					"Please provide the required arguments: \n"
			    + "1: Path to the directory where the data sets are located, "
			    + "2: Algorithm to be executed, or all to execute all algorithms \n"
			    + "3: Execution mode (Cluster/Local)");
		}
  
    Config.PARENT_DIR = args(0)
    val algo = args(1)
    val mode = args(2)
    
    val env = ExecutionEnvironment.getExecutionEnvironment
    
    var start : Long = 0
    var end : Long = 0
    var exeTime: Long = 0
    var accuracy : Double = 0
    
    /**
     * Log File to write the execution time and accuracy of algorithms.
     * And testing the algorithms to find their accuracy and execution time.
     */
    try {
      val file = new File("FlinkMLOutput-" + mode + ".txt")
      val bw = new FileWriter(file, true)
      bw.write("FlinkML Accuracy and Execution Time Results \n")
      if(algo == "all" | algo == "MLR"){
        start = System.currentTimeMillis()
        val regression = new MLRegression(env)
        val MLRAccuracy = regression.execute()
        end = System.currentTimeMillis()
        exeTime = (end - start)
        bw.write("MultipleLinearRegression, " + ", " + exeTime + ", " + MLRAccuracy + "\n")
      }
      
      if(algo == "all" | algo == "SVM"){
        start = System.currentTimeMillis()
        val classification = new SVMClassification(env)
        val SVMAccuracy = classification.execute()
        end = System.currentTimeMillis()
        exeTime = (end - start)
        bw.write("SVMClassification, "  + exeTime + ", " + SVMAccuracy + "\n")
      }

      if(algo == "all" | algo == "ALS"){
        start = System.currentTimeMillis()
        val recommendation = new ALSRating(env)
        val ALSAccuracy = recommendation.execute()
        end = System.currentTimeMillis()
        exeTime = (end - start)
         bw.write("ALSRating, " + exeTime + ", " + ALSAccuracy + "\n")
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
