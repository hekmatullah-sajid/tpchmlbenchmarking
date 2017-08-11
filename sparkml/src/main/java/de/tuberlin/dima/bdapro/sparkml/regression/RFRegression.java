package de.tuberlin.dima.bdapro.sparkml.regression;

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.VectorIndexer;
import org.apache.spark.ml.feature.VectorIndexerModel;
import org.apache.spark.ml.regression.RandomForestRegressionModel;
import org.apache.spark.ml.regression.RandomForestRegressor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class RFRegression {

	public RFRegression() {
		// TODO Auto-generated constructor stub
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		
		long start = System.currentTimeMillis();
		SparkSession spark = SparkSession
			      .builder()
			     // .master("local[2]")
			      .appName("JavaRandomForestRegressor")
			      .getOrCreate();

			    // $example on$
			    // Load and parse the data file, converting it to a DataFrame.
		String path = args[0];
			    Dataset<Row> data = spark.read().format("libsvm")
					      .load(path).cache();

			    // Automatically identify categorical features, and index them.
			    // Set maxCategories so features with > 4 distinct values are treated as continuous.
			    VectorIndexerModel featureIndexer = new VectorIndexer()
			      .setInputCol("features")
			      .setOutputCol("indexedFeatures")
			      .setMaxCategories(4)
			      .fit(data);

			    // Split the data into training and test sets (20% held out for testing)
			    Dataset<Row>[] splits = data.randomSplit(new double[] {0.8, 0.2});
			    Dataset<Row> trainingData = splits[0];
			    Dataset<Row> testData = splits[1];

			    // Train a RandomForest model.
			    RandomForestRegressor rf = new RandomForestRegressor()
			      .setLabelCol("label")
			      .setFeaturesCol("indexedFeatures");

			    // Chain indexer and forest in a Pipeline
			    Pipeline pipeline = new Pipeline()
			      .setStages(new PipelineStage[] {featureIndexer, rf});

			    // Train model. This also runs the indexer.
			    PipelineModel model = pipeline.fit(trainingData);

			    // Make predictions.
			    Dataset<Row> predictions = model.transform(testData);

			    // Select example rows to display.
			    predictions.select("prediction", "label", "features").show(5);

			    // Select (prediction, true label) and compute test error
			    RegressionEvaluator evaluator = new RegressionEvaluator()
			      .setLabelCol("label")
			      .setPredictionCol("prediction")
			      .setMetricName("rmse");
			    double rmse = evaluator.evaluate(predictions);
			    System.out.println("Root Mean Squared Error (RMSE) on test data = " + rmse);

			    RandomForestRegressionModel rfModel = (RandomForestRegressionModel)(model.stages()[1]);
			    System.out.println("Learned regression forest model:\n" + rfModel.toDebugString());
			    // $example off$

			    spark.stop();
				long end = System.currentTimeMillis();
				 System.out.println("The toatl time was :\n" + (start - end));
	}

}
