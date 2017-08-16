package de.tuberlin.dima.bdapro.sparkml.regression;

import de.tuberlin.dima.bdapro.sparkml.Config;
import de.tuberlin.dima.bdapro.sparkml.MLAlgorithmBase;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.ml.regression.LinearRegressionTrainingSummary;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class GeneralizedLinearRegression extends MLAlgorithmBase {

	public GeneralizedLinearRegression(final SparkSession spark) {
		super(spark);
	}

	public void execute() {
		
		//		  SparkSession spark = SparkSession
		//			      .builder()
		//			      .master("local[2]")
		//			      .appName("JavaGeneralizedLinearRegression")
		//			      .getOrCreate();
		String inputfile = Config.pathToRegressionTrainingSet();
		Dataset<Row> training = spark.read().format("libsvm")
		  .load(inputfile);

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
		//				System.out.println("r2: " + trainingSummary.r2());
		//
		//			    spark.stop();

	}

}
