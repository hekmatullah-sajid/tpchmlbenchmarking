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

public class NaiveBayesClassification extends MLAlgorithmBase{
    public NaiveBayesClassification(final SparkSession spark)
    {
        super(spark);
    }
    public double execute() {
//        SparkSession spark = SparkSession
//                .builder()
//                .appName("JavaNaiveBayesExample")
//                .config("spark.master", "local")
//                .getOrCreate();

        // $example on$
        // Load training data
        String inputfile = Config.pathToClassificationTrainingSet();
        Dataset<Row> dataFrame =
                spark.read().format("libsvm").load(inputfile);
        // Split the data into train and test
        Dataset<Row>[] splits = dataFrame.randomSplit(new double[]{0.6, 0.4}, 1234L);
        Dataset<Row> train = splits[0];
        Dataset<Row> test = splits[1];

        // create the trainer and set its parameters
        NaiveBayes nb = new NaiveBayes();

        // train the model
        NaiveBayesModel model = nb.fit(train);

        // Select example rows to display.
        Dataset<Row> predictions = model.transform(test);
        predictions.show();

        // compute accuracy on the test set
        MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator()
                .setLabelCol("label")
                .setPredictionCol("prediction")
                .setMetricName("accuracy");
        double accuracy = evaluator.evaluate(predictions);
        System.out.println("Test set accuracy = " + accuracy);
        return accuracy;
        // $example off$

        //spark.stop();
    }
}
