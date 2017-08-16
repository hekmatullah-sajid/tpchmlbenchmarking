package de.tuberlin.dima.bdapro.sparkml;

import de.tuberlin.dima.bdapro.sparkml.classification.*;
import de.tuberlin.dima.bdapro.sparkml.clustering.KMeansClustering;
import de.tuberlin.dima.bdapro.sparkml.recommendation.ALSRating;
import de.tuberlin.dima.bdapro.sparkml.regression.DecisionTreeRegression;
import de.tuberlin.dima.bdapro.sparkml.regression.GeneralizedLinearRegression;
import de.tuberlin.dima.bdapro.sparkml.regression.RandomForestRegression;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by seema on 15.08.17.
 */
public class BenchmarkingJob {

    public static void main(final String[] args) {
        ////////////////////////ARGUMENT PARSING ///////////////////////////////
        if (args.length <= 0 || args.length > 2) {
            throw new IllegalArgumentException(
                    "Please input the path to the directory where the test databases are located.");
        }
        String path = args[0];
        try {
            File file = new File(path);
            if (!(file.isDirectory() && file.exists() || path.contains("hdfs"))) {
                throw new IllegalArgumentException(
                        "Please give a valid path to the directory where the test databases are located.");
            }
        } catch (Exception ex) {
            throw new IllegalArgumentException(
                    "Please give a valid path to the directory where the test databases are located.");
        }


        //Benchmarking ML Algorithms runtime and error rate
        SparkSession spark = SparkSession.builder()
                .appName("ML Spark Batch Benchmarking").config("spark.master", "local").getOrCreate();
        Config.SetBaseDir(path);

        // Variables to store Benchmarking intermediate and final values
        List<String> results = new ArrayList<String>();
        long start = 0;
        long end = 0;

//        start = System.currentTimeMillis();
//        final DecisionTreeClassification dtClassification = new DecisionTreeClassification(spark);
//        dtClassification.execute();
//        end = System.currentTimeMillis();
//        results.add(" DecisionTreeClassification|" + (end - start) + "\r\n");
//
//        start = System.currentTimeMillis();
//        final GradientBoostedTreeClassification gbtClassification = new GradientBoostedTreeClassification(spark);
//        gbtClassification.execute();
//        end = System.currentTimeMillis();
//        results.add(" GradientBoostedTreeClassification|" + (end - start) + "\r\n");
//
//        start = System.currentTimeMillis();
//        final NaiveBayesClassification nbClassification = new NaiveBayesClassification(spark);
//        nbClassification.execute();
//        end = System.currentTimeMillis();
//        results.add(" NaiveBayesClassification|" + (end - start) + "\r\n");
//
//        start = System.currentTimeMillis();
//        final RandomForestClassification rfClassification = new RandomForestClassification(spark);
//        rfClassification.execute();
//        end = System.currentTimeMillis();
//        results.add(" RandomForestClassification|" + (end - start) + "\r\n");
//
//        start = System.currentTimeMillis();
//        final SVMClassification svmClassification = new SVMClassification(spark);
//        svmClassification.execute();
//        end = System.currentTimeMillis();
//        results.add(" SVMClassification|" + (end - start) + "\r\n");
//
//        start = System.currentTimeMillis();
//        final KMeansClustering kmeansClustering = new KMeansClustering(spark);
//        kmeansClustering.execute();
//        end = System.currentTimeMillis();
//        results.add(" KMeansClustering|" + (end - start) + "\r\n");
//
//        start = System.currentTimeMillis();
//        final ALSRating alsRating = new ALSRating(spark);
//        alsRating.execute();
//        end = System.currentTimeMillis();
//        results.add(" ALSRating|" + (end - start) + "\r\n");

        start = System.currentTimeMillis();
        final DecisionTreeRegression dtRegression = new DecisionTreeRegression(spark);
        dtRegression.execute();
        end = System.currentTimeMillis();
        results.add(" DecisionTreeRegression|" + (end - start) + "\r\n");

        start = System.currentTimeMillis();
        final GeneralizedLinearRegression glRegression = new GeneralizedLinearRegression(spark);
        glRegression.execute();
        end = System.currentTimeMillis();
        results.add(" GeneralizedLinearRegression|" + (end - start) + "\r\n");

        start = System.currentTimeMillis();
        final RandomForestRegression rfRegression = new RandomForestRegression(spark);
        rfRegression.execute();
        end = System.currentTimeMillis();
        results.add(" RandomForestRegression|" + (end - start) + "\r\n");


        //write the output to a file
        try {
            FileWriter writer = new FileWriter("SparkMLOutput.txt", true);
            for (String str : results) {
                writer.write(str);
            }
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        spark.stop();
    }
}
