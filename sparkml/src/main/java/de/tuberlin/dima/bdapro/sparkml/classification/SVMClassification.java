package de.tuberlin.dima.bdapro.sparkml.classification;

import de.tuberlin.dima.bdapro.sparkml.Config;
import de.tuberlin.dima.bdapro.sparkml.MLAlgorithmBase;
import org.apache.spark.ml.classification.LinearSVC;
import org.apache.spark.ml.classification.LinearSVCModel;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Class for testing the SVM Classification ML algorithm
 * 
 * @author Seema Narasimha Swamy
 *
 */
public class SVMClassification extends MLAlgorithmBase{
    public SVMClassification(final SparkSession spark)
    {
        super(spark);
    }
    
    /**
     * 
     * The execute method is used to test the algorithm.
     * The input data set is in libsvm format which is split into two parts 70% for learning and the rest for testing.
     * The method returns "accuracy" for the algorithm.
     * 
     */
    public double execute() {
    	
        /*
         * Load training data.
         */
        String inputfile = Config.pathToClassificationTrainingSet();
        Dataset<Row> data = spark.read().format("libsvm")
                .load(inputfile).cache();

        LinearSVC lsvc = new LinearSVC()
                .setMaxIter(10)
                .setRegParam(0.1);
        
        /*
         *  Split the data into training and test sets (30% held out for testing)
         */
        Dataset<Row>[] splits = data.randomSplit(new double[] {0.7, 0.3});
        Dataset<Row> trainingData = splits[0];
        Dataset<Row> testData = splits[1];

        /*
         *  Fit the model
         */
        LinearSVCModel lsvcModel = lsvc.fit(trainingData);
        
        /*
         * Make predictions.
         */
        Dataset<Row> predictions = lsvcModel.transform(testData);

        /*
         * Evaluate the algorithm and find the test error.
         */
        MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator()
                .setLabelCol("label")
                .setPredictionCol("prediction")
                .setMetricName("accuracy");
        double accuracy = evaluator.evaluate(predictions);
        return accuracy;
    }
}
