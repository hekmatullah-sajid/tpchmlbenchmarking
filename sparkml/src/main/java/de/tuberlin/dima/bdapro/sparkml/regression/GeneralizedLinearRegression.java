package de.tuberlin.dima.bdapro.sparkml.regression;

import de.tuberlin.dima.bdapro.sparkml.Config;
import de.tuberlin.dima.bdapro.sparkml.MLAlgorithmBase;

import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.ml.regression.LinearRegressionTrainingSummary;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Class for testing the  Generalized Linear Regression ML algorithm.
 * 
 * @author Hekmatullah Sajid
 *
 */
public class GeneralizedLinearRegression extends MLAlgorithmBase {

	public GeneralizedLinearRegression(final SparkSession spark) {
		super(spark);
	}

	/**
     * 
     * The execute method is used to test the algorithm.
     * The input data set is in libsvm format which is split into two parts 80% for learning and the rest for testing.
     * The method returns "Root Mean Squared Error (RMSE)" for the algorithm.
     * 
     */
	public double execute() {
		
		/*
         * Load the data stored in LIBSVM format as a DataFrame.
         */
		String inputfile = Config.pathToRegressionTrainingSet();
		Dataset<Row> data = spark.read().format("libsvm")
		  .load(inputfile).cache();

		/*
		 * Train a LinearRegression model.
		 */
		LinearRegression lr = new LinearRegression()
		  .setMaxIter(20)
		  .setRegParam(0.3)
		  .setElasticNetParam(0.8);

		
		/*
		 * Split the data into training and test sets (80% training and 20% held for testing).
		 */
        Dataset<Row>[] splits = data.randomSplit(new double[]{0.8, 0.2});
        Dataset<Row> trainingData = splits[0];
        Dataset<Row> testData = splits[1];
        
        /*
         * Fit the model.
         */
     	LinearRegressionModel lrModel = lr.fit(trainingData);
     	
     	/*
     	 * Predict the testData
     	 */
     	lrModel.transform(testData);
     	
     	/*
     	 * Summarize the model over the training set and find RMSE.
     	 */
		LinearRegressionTrainingSummary trainingSummary = lrModel.summary();
		trainingSummary.residuals();
		return trainingSummary.rootMeanSquaredError();

	}

}
