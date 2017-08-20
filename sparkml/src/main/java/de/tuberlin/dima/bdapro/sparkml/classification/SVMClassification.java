package de.tuberlin.dima.bdapro.sparkml.classification;

/**
 * Created by seema on 08.08.17.
 */

import de.tuberlin.dima.bdapro.sparkml.Config;
import de.tuberlin.dima.bdapro.sparkml.MLAlgorithmBase;
import org.apache.spark.ml.classification.LinearSVC;
import org.apache.spark.ml.classification.LinearSVCModel;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SVMClassification extends MLAlgorithmBase{
    public SVMClassification(final SparkSession spark)
    {
        super(spark);
    }
    public double execute() {
//        SparkSession spark = SparkSession
//                .builder()
//                .appName("JavaLinearSVCExample")
//                .config("spark.master", "local")
//                .getOrCreate();

        // $example on$
        // Load training data
        String inputfile = Config.pathToClassificationTrainingSet();
        Dataset<Row> training = spark.read().format("libsvm")
                .load(inputfile).cache();

        LinearSVC lsvc = new LinearSVC()
                .setMaxIter(10)
                .setRegParam(0.1);

        // Fit the model
        LinearSVCModel lsvcModel = lsvc.fit(training);
        Dataset<Row> predictions = lsvcModel.transform(training);
        predictions.show();

        // compute accuracy on the test set
        MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator()
                .setLabelCol("label")
                .setPredictionCol("prediction")
                .setMetricName("accuracy");
        double accuracy = evaluator.evaluate(predictions);
        System.out.println("Test set accuracy = " + accuracy);
        return accuracy;
        // Print the coefficients and intercept for LinearSVC
//        System.out.println("Coefficients: "
//                + lsvcModel.coefficients() + " Intercept: " + lsvcModel.intercept());
//        // $example off$
//
//        spark.stop();
    }
}
