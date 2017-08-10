package de.tuberlin.dima.bdapro.sparkml;

import java.util.HashMap;
import java.util.Map;

import scala.Tuple2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.DecisionTree;
import org.apache.spark.mllib.tree.model.DecisionTreeModel;
import org.apache.spark.mllib.util.MLUtils;

public class DecisionTR {

	public DecisionTR() {
		// TODO Auto-generated constructor stub
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("JavaDecisionTreeRegression");
	    JavaSparkContext javaSC = new JavaSparkContext(sparkConf);

	    // Load and parse the data file (hour.csv).
	    String path = "C:\\sample_libsvm_data.txt";
//	    JavaRDD<String> data = javaSC.textFile(path);
//	    JavaRDD<LabeledPoint> parsedData = data.map(line -> {
//	      String[] parts = line.split(",");
//	      String[] features = parts[1].split(" ");
//	      double[] v = new double[features.length];
//	      for (int i = 0; i < features.length - 1; i++) {
//	        v[i] = Double.parseDouble(features[i]);
//	      }
//	      return new LabeledPoint(Double.parseDouble(parts[0]), Vectors.dense(v));
//	    });
	    
	    JavaRDD<LabeledPoint> parsedData = MLUtils.loadLibSVMFile(javaSC.sc(), path).toJavaRDD();
	    // Split the data into training and test sets (30% held out for testing)
	    JavaRDD<LabeledPoint>[] splits = parsedData.randomSplit(new double[]{0.7, 0.3});
	    JavaRDD<LabeledPoint> trainingData = splits[0];
	    JavaRDD<LabeledPoint> testData = splits[1];

	    // Set parameters.
	    // Empty categoricalFeaturesInfo indicates all features are continuous.
	    Map<Integer, Integer> categoricalFeaturesInfo = new HashMap<>();
	    String impurity = "variance";
	    int maxDepth = 5;
	    int maxBins = 32;

	    // Train a DecisionTree model.
	    DecisionTreeModel model = DecisionTree.trainRegressor(trainingData,
	      categoricalFeaturesInfo, impurity, maxDepth, maxBins);

	    // Evaluate model on test instances and compute test error
	    JavaPairRDD<Double, Double> predictionAndLabel =
	      testData.mapToPair(p -> new Tuple2<>(model.predict(p.features()), p.label()));
	    double testMSE = predictionAndLabel.mapToDouble(pl -> {
	      double diff = pl._1() - pl._2();
	      return diff * diff;
	    }).mean();
	    System.out.println("Test Mean Squared Error: " + testMSE);
	    System.out.println("Learned regression tree model:\n" + model.toDebugString());

	    // Save and load model
	    model.save(javaSC.sc(), "C:\\tmp\\DecisionTreeRegressionModel");
	    DecisionTreeModel sameModel = DecisionTreeModel
	      .load(javaSC.sc(), "C:\\tmp\\DecisionTreeRegressionModel");
	    
	    javaSC.stop();

	}

}
