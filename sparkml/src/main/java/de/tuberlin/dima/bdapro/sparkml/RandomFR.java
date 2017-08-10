package de.tuberlin.dima.bdapro.sparkml;

import java.util.HashMap;
import java.util.Map;

import scala.Tuple2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.RandomForest;
import org.apache.spark.mllib.tree.model.RandomForestModel;
import org.apache.spark.mllib.util.MLUtils;
import org.apache.spark.SparkConf;

public class RandomFR {

	public RandomFR() {
		// TODO Auto-generated constructor stub
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("RandomForestRegression");
	    JavaSparkContext javaSC = new JavaSparkContext(sparkConf);
	    // Load and parse the data file.
	    String datapath = "C:\\sample_libsvm_data.txt";
	    JavaRDD<LabeledPoint> data = MLUtils.loadLibSVMFile(javaSC.sc(), datapath).toJavaRDD();
	    // Split the data into training and test sets (30% held out for testing)
	    JavaRDD<LabeledPoint>[] splits = data.randomSplit(new double[]{0.7, 0.3});
	    JavaRDD<LabeledPoint> trainingData = splits[0];
	    JavaRDD<LabeledPoint> testData = splits[1];
	
	    // Set parameters.
	    // Empty categoricalFeaturesInfo indicates all features are continuous.
	    Map<Integer, Integer> categoricalFeaturesInfo = new HashMap<>();
	    int numTrees = 7; // Use more in practice.
	    String featureSubsetStrategy = "auto"; // Let the algorithm choose.
	    String impurity = "variance";
	    int maxDepth = 4;
	    int maxBins = 32;
	    int seed = 12345;
	    // Train a RandomForest model.
	    RandomForestModel model = RandomForest.trainRegressor(trainingData,
	      categoricalFeaturesInfo, numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins, seed);
	
	    // Evaluate model on test instances and compute test error
	    JavaPairRDD<Double, Double> predictionAndLabel =
	      testData.mapToPair(p -> new Tuple2<>(model.predict(p.features()), p.label()));
	    double testMSE = predictionAndLabel.mapToDouble(pl -> {
	      double diff = pl._1() - pl._2();
	      return diff * diff;
	    }).mean();
	    System.out.println("Test Mean Squared Error: " + testMSE);
	    System.out.println("Learned regression forest model:\n" + model.toDebugString());
	
	    // Save and load model
	    model.save(javaSC.sc(), "C:\\tmp\\DecisionTreeRegressionModel");
	    RandomForestModel sameModel = RandomForestModel.load(javaSC.sc(),
	      "C:\\tmp\\DecisionTreeRegressionModel");
	    // $example off$
	
	    javaSC.stop();
	}

}
