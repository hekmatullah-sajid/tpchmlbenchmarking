package de.tuberlin.dima.bdapro.sparkml;

import de.tuberlin.dima.bdapro.sparkml.classification.*;
import de.tuberlin.dima.bdapro.sparkml.clustering.KMeansClustering;
import de.tuberlin.dima.bdapro.sparkml.recommendation.ALSRating;
import de.tuberlin.dima.bdapro.sparkml.regression.DecisionTreeRegression;
import de.tuberlin.dima.bdapro.sparkml.regression.GeneralizedLinearRegression;
import de.tuberlin.dima.bdapro.sparkml.regression.RandomForestRegression;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Main class of the project for testing ML Algorithms on Apache Spark. 
 * How to run the jar on Spark: ./bin/spark-submit --class de.tuberlin.dima.bdapro.sparkml.BenchmarkingJob -cluster-specification path-to-jar path-to-datasets algorithm mode
 * 
 * @author Hekmatullah Sajid and Seema Narasimha Swamy
 *
 */
public class BenchmarkingJob {

	public static void main(final String[] args) {
		
		/**
		 * ARGUMENT PARSING The main method expects minimum three arguments. 
		 * First argument should be the path to directory where data sets are located.
		 * Second argument should be the all or abbreviation of the algorithm to be executed.
		 * Third argument should be the execution mode (Cluster/Local)
		 */
		if (args.length <= 0 || args.length > 3) {
			throw new IllegalArgumentException("Please provide the required arguments: \n"
			    + "1: Path to the directory where the data sets are located, "
			    + "2: Algorithm to be executed, or all to execute all algorithms \n"
			    + "3: Execution mode (Cluster/Local)");
		}
		String path = args[0];
		try {
			File file = new File(path);
			if (!(file.isDirectory() && file.exists() || path.contains("hdfs"))) {
				throw new IllegalArgumentException(
						"Please give a valid path to the directory where the test databases are located.");
			}
		} catch (Exception ex) {
			throw new IllegalArgumentException(
					"Please give a valid path to the directory where the test databases are located.");
		}

		// Benchmarking ML Algorithms runtime and error rate
		SparkSession spark = SparkSession.builder().appName("ML Spark Batch Benchmarking").getOrCreate(); 																							
		Config.SetBaseDir(path);
		String algo = args[1];
		String mode = args[2];

		/*
		 * The results list is used to store the execution time and accuracy of ML Algorithms.
		 * The start and end variables are used to store the starting and ending time of executing an algorithm,
		 * the difference of these two is written to the result with its corresponding algorithm name and accuracy.
		 */
		List<String> results = new ArrayList<String>();
		long start = 0;
		long end = 0;
		double accuracy = 0;
		// Start collecting the output
		results.add(" Algorithm, execution time, accuracy\r\n");

		/*
		 * Testing the algorithms for their execution time and accuracy.
		 */
		if (algo.equals("all") || algo.equals("DTC")) {
			start = System.currentTimeMillis();
			final DecisionTreeClassification dtClassification = new DecisionTreeClassification(spark);
			accuracy = dtClassification.execute();
			end = System.currentTimeMillis();
			results.add(" DecisionTreeClassification," + (end - start) + "," + accuracy + "\r\n");
		}

		if (algo.equals("all") || algo.equals("GBTC")) {
			start = System.currentTimeMillis();
			final GradientBoostedTreeClassification gbtClassification = new GradientBoostedTreeClassification(spark);
			accuracy = gbtClassification.execute();
			end = System.currentTimeMillis();
			results.add(" GradientBoostedTreeClassification," + (end - start) + "," + accuracy + "\r\n");
		}

		if (algo.equals("all") || algo.equals("NBC")) {
			start = System.currentTimeMillis();
			final NaiveBayesClassification nbClassification = new NaiveBayesClassification(spark);
			accuracy = nbClassification.execute();
			end = System.currentTimeMillis();
			results.add(" NaiveBayesClassification," + (end - start) + "," + accuracy + "\r\n");
		}

		if (algo.equals("all") || algo.equals("RFC")) {
			start = System.currentTimeMillis();
			final RandomForestClassification rfClassification = new RandomForestClassification(spark);
			accuracy = rfClassification.execute();
			end = System.currentTimeMillis();
			results.add(" RandomForestClassification," + (end - start) + "," + accuracy + "\r\n");
		}

		if (algo.equals("all") || algo.equals("SVMC")) {
			start = System.currentTimeMillis();
			final SVMClassification svmClassification = new SVMClassification(spark);
			accuracy = svmClassification.execute();
			end = System.currentTimeMillis();
			results.add(" SVMClassification," + (end - start) + "," + accuracy + "\r\n");
		}

		if (algo.equals("all") || algo.equals("KMC")) {
			start = System.currentTimeMillis();
			final KMeansClustering kmeansClustering = new KMeansClustering(spark);
			accuracy = kmeansClustering.execute();
			end = System.currentTimeMillis();
			results.add(" KMeansClustering," + (end - start) + "," + accuracy + "\r\n");
		}

		if (algo.equals("all") || algo.equals("ALSR")) {
			start = System.currentTimeMillis();
			final ALSRating alsRating = new ALSRating(spark);
			accuracy = alsRating.execute();
			end = System.currentTimeMillis();
			results.add(" ALSRating," + (end - start) + "," + accuracy + "\r\n");
		}

		if (algo.equals("all") || algo.equals("DTR")) {
			start = System.currentTimeMillis();
			final DecisionTreeRegression dtRegression = new DecisionTreeRegression(spark);
			accuracy = dtRegression.execute();
			end = System.currentTimeMillis();
			results.add(" DecisionTreeRegression," + (end - start) + "," + accuracy + "\r\n");
		}

		if (algo.equals("all") || algo.equals("GLR")) {
			start = System.currentTimeMillis();
			final GeneralizedLinearRegression glRegression = new GeneralizedLinearRegression(spark);
			accuracy = glRegression.execute();
			end = System.currentTimeMillis();
			results.add(" GeneralizedLinearRegression," + (end - start) + "," + accuracy + "\r\n");
		}

		if (algo.equals("all") || algo.equals("RFR")) {
			start = System.currentTimeMillis();
			final RandomForestRegression rfRegression = new RandomForestRegression(spark);
			accuracy = rfRegression.execute();
			end = System.currentTimeMillis();
			results.add(" RandomForestRegression," + (end - start) + "," + accuracy + "\r\n");
		}

		/**
	     * Log File to write the execution time and accuracy of algorithms.
	     */		
		try {
			FileWriter writer = new FileWriter("SparkMLOutput-" + mode + ".txt", true);
			for (String str : results) {
				writer.write(str);
			}
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		spark.stop();
	}
}
