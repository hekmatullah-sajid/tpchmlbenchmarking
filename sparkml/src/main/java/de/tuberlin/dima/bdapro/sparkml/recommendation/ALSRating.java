package de.tuberlin.dima.bdapro.sparkml.recommendation;

import de.tuberlin.dima.bdapro.sparkml.Config;
import de.tuberlin.dima.bdapro.sparkml.MLAlgorithmBase;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import java.io.Serializable;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.recommendation.ALS;
import org.apache.spark.ml.recommendation.ALSModel;

/**
 * Class for testing the ALS Rating (Recommendation) ML algorithm
 * 
 * @author Seema Narasimha Swamy
 *
 */
public class ALSRating extends MLAlgorithmBase {
	public ALSRating(final SparkSession spark) {
		super(spark);
	}

	public static class Rating implements Serializable {
		private static final long serialVersionUID = 1L;
		
		private int userId;
		private int movieId;
		private float rating;
		private long timestamp;

		public Rating() {
		}

		public Rating(int userId, int movieId, float rating, long timestamp) {
			this.userId = userId;
			this.movieId = movieId;
			this.rating = rating;
			this.timestamp = timestamp;
		}

		public int getUserId() {
			return userId;
		}

		public int getMovieId() {
			return movieId;
		}

		public float getRating() {
			return rating;
		}

		public long getTimestamp() {
			return timestamp;
		}

		public static Rating parseRating(String str) {
			String[] fields = str.split(",");
			if (fields.length != 4) {
				throw new IllegalArgumentException("Each line must contain 4 fields");
			}
			int userId = Integer.parseInt(fields[0]);
			int movieId = Integer.parseInt(fields[1]);
			float rating = Float.parseFloat(fields[2]);
			long timestamp = Long.parseLong(fields[3]);
			return new Rating(userId, movieId, rating, timestamp);
		}
	}

	
	/**
     * 
     * The execute method is used to test the algorithm.
     * The input data set (movielens) is in CSV format which is split into two parts 80% for learning and the rest for testing.
     * The method returns "Root Mean Square Error" for the algorithm.
     * 
     */
	public double execute() {
		/**
		 * Read input data set from a csv file
		 */
		String inputfile = Config.pathToRecommendationTrainingSet();
		JavaRDD<Rating> ratingsRDD = spark.read().textFile(inputfile).javaRDD().map(Rating::parseRating).cache();
		Dataset<Row> ratings = spark.createDataFrame(ratingsRDD, Rating.class);
		
		/*
		 * Split the data into training and test sets (80% training and 20% held for testing).
		 */
		Dataset<Row>[] splits = ratings.randomSplit(new double[] { 0.8, 0.2 });
		Dataset<Row> training = splits[0];
		Dataset<Row> test = splits[1];

		/*
		 *  Build the recommendation model using ALS on the training data
		 */
		ALS als = new ALS().setMaxIter(5).setRegParam(0.01).setUserCol("userId").setItemCol("movieId")
				.setRatingCol("rating");
		ALSModel model = als.fit(training);

		/*
		 * Evaluate the model by computing the RMSE on the test data
		 * Note we set cold start strategy to 'drop' to ensure we don't get NaN
		 * evaluation metrics
		 * 
		 */ 
		model.setColdStartStrategy("drop");
		
		/*
         * Make predictions.
         */
		Dataset<Row> predictions = model.transform(test);

		/*
         *  Evaluate prediction and compute Root Mean Square Error.
         */
		RegressionEvaluator evaluator = new RegressionEvaluator().setMetricName("rmse").setLabelCol("rating")
				.setPredictionCol("prediction");
		Double rmse = evaluator.evaluate(predictions);
		
		return rmse;
	}
}
