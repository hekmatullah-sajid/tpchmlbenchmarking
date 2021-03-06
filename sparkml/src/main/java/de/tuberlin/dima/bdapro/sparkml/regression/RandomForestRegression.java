package de.tuberlin.dima.bdapro.sparkml.regression;

import de.tuberlin.dima.bdapro.sparkml.Config;
import de.tuberlin.dima.bdapro.sparkml.MLAlgorithmBase;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.VectorIndexer;
import org.apache.spark.ml.feature.VectorIndexerModel;
import org.apache.spark.ml.regression.RandomForestRegressor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Class for testing the Random Forest Regression ML algorithm.
 * 
 * @author Hekmatullah Sajid
 *
 */
public class RandomForestRegression extends MLAlgorithmBase{

	public RandomForestRegression(final SparkSession spark) {
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
		 * Load and parse the data file (libsvm input file), converting it to a DataFrame.
		 */
		String path = Config.pathToRegressionTrainingSet();
		Dataset<Row> data = spark.read().format("libsvm")
				  .load(path).cache();

		/*
		 * Automatically identify categorical features, and index them.
		 * Set maxCategories so features with > 4 distinct values are treated as continuous.
		 */
		VectorIndexerModel featureIndexer = new VectorIndexer()
		  .setInputCol("features")
		  .setOutputCol("indexedFeatures")
		  .setMaxCategories(4)
		  .fit(data);

		/*
		 * Split the data into training and test sets (80% training and 20% held for testing).
		 */
		Dataset<Row>[] splits = data.randomSplit(new double[] {0.8, 0.2});
		Dataset<Row> trainingData = splits[0];
		Dataset<Row> testData = splits[1];

		/*
		 * Train a RandomForest model.
		 */
		RandomForestRegressor rf = new RandomForestRegressor()
		  .setLabelCol("label")
		  .setFeaturesCol("indexedFeatures");

		/*
		 * Chain indexer and forest in a Pipeline.
		 */
		Pipeline pipeline = new Pipeline()
		  .setStages(new PipelineStage[] {featureIndexer, rf});

		/*
		 * Train model. 
		 * This also runs the indexer.
		 */
		PipelineModel model = pipeline.fit(trainingData);

		/*
		 * Make predictions.
		 */
		Dataset<Row> predictions = model.transform(testData);

		/*
		 * Select (prediction, true label) and compute test error
		 */
		RegressionEvaluator evaluator = new RegressionEvaluator()
		  .setLabelCol("label")
		  .setPredictionCol("prediction")
		  .setMetricName("rmse");
		double rmse = evaluator.evaluate(predictions);
		return rmse;
	}

}
