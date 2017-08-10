package de.tuberlin.dima.bdapro.sparkml;

import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class KMeansAlogrithm {

	public KMeansAlogrithm() {
		// TODO Auto-generated constructor stub
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		
		 SparkSession spark = SparkSession
			      .builder()
			      .master("local[2]")
			      .appName("KMeans")
			      .getOrCreate();

			    // $example on$
			    // Loads data.
			    Dataset<Row> dataset = spark.read().format("libsvm")
			    		.load("C:\\kmeans_data.txt");

			    // Trains a k-means model.
			    KMeans kmeans = new KMeans().setK(2).setSeed(1L);
			    KMeansModel model = kmeans.fit(dataset);

			    // Evaluate clustering by computing Within Set Sum of Squared Errors.
			    double WSSSE = model.computeCost(dataset);
			    System.out.println("Within Set Sum of Squared Errors = " + WSSSE);

			    // Shows the result.
			    Vector[] centers = model.clusterCenters();
			    System.out.println("Cluster Centers: ");
			    for (Vector center: centers) {
			      System.out.println(center);
			    }
			    // $example off$

			    spark.stop();
			    

	}

}
