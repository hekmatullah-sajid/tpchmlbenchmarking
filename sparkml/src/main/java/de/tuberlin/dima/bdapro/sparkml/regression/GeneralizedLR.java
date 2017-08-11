//package de.tuberlin.dima.bdapro.sparkml.regression;
//
//import java.util.Arrays;
//
//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.api.java.JavaSparkContext;
//import org.apache.spark.ml.regression.GeneralizedLinearRegression;
//import org.apache.spark.ml.regression.GeneralizedLinearRegressionModel;
//import org.apache.spark.ml.regression.GeneralizedLinearRegressionTrainingSummary;
//import org.apache.spark.sql.Dataset;
//import org.apache.spark.sql.Row;
//
//import scala.Tuple2;
//
//import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.JavaPairRDD;
//import org.apache.spark.mllib.classification.LogisticRegressionModel;
//import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS;
//import org.apache.spark.mllib.evaluation.MulticlassMetrics;
//import org.apache.spark.mllib.linalg.Vectors;
//import org.apache.spark.mllib.regression.LabeledPoint;
//import org.apache.spark.mllib.regression.LinearRegressionModel;
//import org.apache.spark.mllib.regression.LinearRegressionWithSGD;
//import org.apache.spark.mllib.util.MLUtils;
//// $example off$
//import org.apache.spark.sql.SparkSession;
//
//public class GeneralizedLR {
//
//	  public static void main(String[] args) {
////		    SparkSession spark = SparkSession
////		      .builder()
////		      .appName("JavaGeneralizedLinearRegressionExample")
////		      .master("local")
////		      .getOrCreate();
//		    
//		    SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("JavaLinearRegressionWithSGD");
//		    JavaSparkContext javaSC = new JavaSparkContext(conf);
////		   
////		    // Load and parse the data file (hour.csv).
////		    String path = "C:\\orgrid.csv";
////		    JavaRDD<String> data = javaSC.textFile(path);
////		    JavaRDD<LabeledPoint> parsedData = data.map(line -> {
////		      String[] parts = line.split(",");
////		      //String[] features = parts[1].split(" ");
////		      //System.out.println(features[0]);
////		      double[] v = new double[parts.length - 1];
////		      for (int i = 1; i < parts.length - 1; i++) {
////		        v[i] = Double.parseDouble(parts[i]);
////		      }
////		      return new LabeledPoint(Double.parseDouble(parts[0]), Vectors.dense(v));
////		    });
////		    parsedData.cache();
////
////		    // Building the model
////		    int numIterations = 100;
////		    double stepSize = 0.00000001;
////		    LinearRegressionModel model =
////		      LinearRegressionWithSGD.train(JavaRDD.toRDD(parsedData), numIterations, stepSize);
////
////		    // Evaluate model on training examples and compute training error
////		    JavaPairRDD<Double, Double> valuesAndPreds = parsedData.mapToPair(point ->
////		      new Tuple2<>(model.predict(point.features()), point.label()));
////
////		    double MSE = valuesAndPreds.mapToDouble(pair -> {
////		      double diff = pair._1() - pair._2();
////		      return diff * diff;
////		    }).mean();
////		    System.out.println("training Mean Squared Error = " + MSE);
////
////		    // Save and load model
////		    model.save(javaSC.sc(), "C:\\javaLinearRegressionWithSGDModel");
////		    LinearRegressionModel sameModel = LinearRegressionModel.load(javaSC.sc(),
////		      "C:\\javaLinearRegressionWithSGDModel");		    
////		    
//		    
//		    
//		    
////
////		    // $example on$
////		    // Load training data
////		    Dataset<Row> dataset = spark.read().format("libsvm")
////		      .load("C:\\sample_linear_regression_data.txt").cache();
////		    
////		    
////		    System.out.println(dataset.first().getDouble(0));
////		    
////		    GeneralizedLinearRegression glr = new GeneralizedLinearRegression()
////		      .setFamily("gaussian")
////		      .setLink("identity")
////		      .setMaxIter(10)
////		      .setRegParam(0.3);
////
////		    // Fit the model
////		    GeneralizedLinearRegressionModel model = glr.fit(dataset);
////
////		    // Print the coefficients and intercept for generalized linear regression model
////		    System.out.println("Coefficients: " + model.coefficients());
////		    System.out.println("Intercept: " + model.intercept());
////
////		    // Summarize the model over the training set and print out some metrics
////		    GeneralizedLinearRegressionTrainingSummary summary = model.summary();
////		    System.out.println("Coefficient Standard Errors: "
////		      + Arrays.toString(summary.coefficientStandardErrors()));
////		    System.out.println("T Values: " + Arrays.toString(summary.tValues()));
////		    System.out.println("P Values: " + Arrays.toString(summary.pValues()));
////		    System.out.println("Dispersion: " + summary.dispersion());
////		    System.out.println("Null Deviance: " + summary.nullDeviance());
////		    System.out.println("Residual Degree Of Freedom Null: " + summary.residualDegreeOfFreedomNull());
////		    System.out.println("Deviance: " + summary.deviance());
////		    System.out.println("Residual Degree Of Freedom: " + summary.residualDegreeOfFreedom());
////		    System.out.println("AIC: " + summary.aic());
////		    System.out.println("Deviance Residuals: ");
////		    summary.residuals().show();
////		    // $example off$
////
//		    javaSC.stop();
//		}
//}