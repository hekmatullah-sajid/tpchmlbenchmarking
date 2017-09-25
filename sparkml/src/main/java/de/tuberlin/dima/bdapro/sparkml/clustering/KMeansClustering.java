package de.tuberlin.dima.bdapro.sparkml.clustering;

import de.tuberlin.dima.bdapro.sparkml.Config;
import de.tuberlin.dima.bdapro.sparkml.MLAlgorithmBase;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Class for testing the KMeans Clustering ML algorithm.
 * 
 * @author Hekmatullah Sajid
 *
 */
public class KMeansClustering extends MLAlgorithmBase{

	public KMeansClustering(final SparkSession spark) {
		super(spark);
	}

	 /**
     * 
     * The execute method is used to test the algorithm.
     * The input data set is in libsvm format.
     * The value of K (60) is chosen using the elbow method.
     * The method returns "Within Set Sum of Squared Errors" for the algorithm.
     * 
     */
	public double execute() {
		/*
		 *  Loads data from the libsvm format dataset.
		 */
		String inputfile = Config.pathToClusteringTrainingSet();
		Dataset<Row> tempdataset = spark.read().format("libsvm")
				.load(inputfile).cache();
		
		/*
		 * The actual dataset used in this algorithm was for classification and clustering both.
		 * Here the label is dropped to use the dataset for clustering.
		 */
		Dataset<Row> dataset = tempdataset.drop("label");

		/*
		 * Training the k-means model.
		 * The value for K is calculated for the intended dataset using the elbow method.
		 */
		KMeans kmeans = new KMeans().setK(60).setSeed(1L);
		
		/*
		 * Fit the model.
		 */
		KMeansModel model = kmeans.fit(dataset);

		/*
		 * Evaluate clustering by computing - Within Set Sum of Squared Errors -.
		 */
		double WSSSE = model.computeCost(dataset);
		return WSSSE;
	}

}
