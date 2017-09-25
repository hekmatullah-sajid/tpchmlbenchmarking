package de.tuberlin.dima.bdapro.sparkml.classification;

/**
 * Created by seema on 07.08.17.
 */

import de.tuberlin.dima.bdapro.sparkml.Config;
import de.tuberlin.dima.bdapro.sparkml.MLAlgorithmBase;
import org.apache.spark.ml.classification.NaiveBayes;
import org.apache.spark.ml.classification.NaiveBayesModel;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Class for testing the Naive Bayes Classification ML algorithm
 * 
 * @author Seema Narasimha Swamy
 *
 */
public class NaiveBayesClassification extends MLAlgorithmBase{
    public NaiveBayesClassification(final SparkSession spark)
    {
        super(spark);
    }
    
    /**
     * 
     * The execute method is used to test the algorithm.
     * The input data set is in libsvm format which is split into two parts 60% for learning and the rest for testing.
     * The method returns "accuracy" for the algorithm.
     * 
     */
    public double execute() {
    	
        /*
         *  Load training data
         */
        String inputfile = Config.pathToNaiveBayesClassificationTrainingSet();
        Dataset<Row> dataFrame =
                spark.read().format("libsvm").load(inputfile).cache();
        /*
         *  Split the data into train and test  (40% held out for testing)
         */
        Dataset<Row>[] splits = dataFrame.randomSplit(new double[]{0.6, 0.4}, 1234L);
        Dataset<Row> train = splits[0];
        Dataset<Row> test = splits[1];

        /*
         *  create the trainer and set its parameters
         */
        NaiveBayes nb = new NaiveBayes();

        /*
         *  train the model
         */
        NaiveBayesModel model = nb.fit(train);

        /*
         * Make predictions.
         */
        Dataset<Row> predictions = model.transform(test);

        /*
         *  compute accuracy on the test set and return.
         */
        MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator()
                .setLabelCol("label")
                .setPredictionCol("prediction")
                .setMetricName("accuracy");
        double accuracy = evaluator.evaluate(predictions);
        return accuracy;
    }
}
