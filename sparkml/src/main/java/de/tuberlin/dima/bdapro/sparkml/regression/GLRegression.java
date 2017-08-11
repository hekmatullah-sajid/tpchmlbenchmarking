package de.tuberlin.dima.bdapro.sparkml.regression;

import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.ml.regression.LinearRegressionTrainingSummary;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class GLRegression {

	public GLRegression() {
		// TODO Auto-generated constructor stub
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		
		  SparkSession spark = SparkSession
			      .builder()
			      .master("local[2]")
			      .appName("JavaGeneralizedLinearRegression")
			      .getOrCreate();

		  Dataset<Row> training = spark.read().format("libsvm")
			      .load("C:\\100lines.txt");

				LinearRegression lr = new LinearRegression()
				  .setMaxIter(20)
				  .setRegParam(0.3)
				  .setElasticNetParam(0.8);

				// Fit the model.
				LinearRegressionModel lrModel = lr.fit(training);

				// Print the coefficients and intercept for linear regression.
				System.out.println("Coefficients: "
				  + lrModel.coefficients() + " Intercept: " + lrModel.intercept());

				// Summarize the model over the training set and print out some metrics.
				LinearRegressionTrainingSummary trainingSummary = lrModel.summary();
				System.out.println("numIterations: " + trainingSummary.totalIterations());
				System.out.println("objectiveHistory: " + Vectors.dense(trainingSummary.objectiveHistory()));
				trainingSummary.residuals().show();
				System.out.println("RMSE: " + trainingSummary.rootMeanSquaredError());
				System.out.println("r2: " + trainingSummary.r2());

			    spark.stop();

	}

}
