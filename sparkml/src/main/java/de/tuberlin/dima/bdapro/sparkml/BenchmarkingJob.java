package de.tuberlin.dima.bdapro.sparkml;

import de.tuberlin.dima.bdapro.sparkml.classification.*;
import de.tuberlin.dima.bdapro.sparkml.clustering.KMeansClustering;
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
                .appName("ML Spark Batch Benchmarking").getOrCreate(); //config("spark.master", "local")
        Config.SetBaseDir(path);

        // Variables to store Benchmarking intermediate and final values
        List<String> results = new ArrayList<String>();
        long start = 0;
        long end = 0;
        double accuracy = 0;
        //Start collecting the outpur
        results.add(" Algorithm, execution time, accuracy\r\n");

        
        start = System.currentTimeMillis();
        final DecisionTreeClassification dtClassification = new DecisionTreeClassification(spark);
        accuracy = dtClassification.execute();
        end = System.currentTimeMillis();
        results.add(" DecisionTreeClassification," + (end - start) + "," + accuracy + "\r\n");

        start = System.currentTimeMillis();
        final GradientBoostedTreeClassification gbtClassification = new GradientBoostedTreeClassification(spark);
        accuracy = gbtClassification.execute();
        end = System.currentTimeMillis();
        results.add(" GradientBoostedTreeClassification," + (end - start) + "," + accuracy + "\r\n");

        start = System.currentTimeMillis();
        final NaiveBayesClassification nbClassification = new NaiveBayesClassification(spark);
        accuracy = nbClassification.execute();
        end = System.currentTimeMillis();
        results.add(" NaiveBayesClassification," + (end - start) + "," + accuracy + "\r\n");

        start = System.currentTimeMillis();
        final RandomForestClassification rfClassification = new RandomForestClassification(spark);
        accuracy = rfClassification.execute();
        end = System.currentTimeMillis();
        results.add(" RandomForestClassification," + (end - start) + "," + accuracy + "\r\n");

//        start = System.currentTimeMillis();
//        final SVMClassification svmClassification = new SVMClassification(spark);
//        accuracy = svmClassification.execute();
//        end = System.currentTimeMillis();
//        results.add(" SVMClassification," + (end - start) + "," + accuracy + "\r\n");

        start = System.currentTimeMillis();
        final KMeansClustering kmeansClustering = new KMeansClustering(spark);
        accuracy = kmeansClustering.execute();
        end = System.currentTimeMillis();
        results.add(" KMeansClustering," + (end - start) + "," + accuracy + "\r\n");

//        start = System.currentTimeMillis();
//        final ALSRating alsRating = new ALSRating(spark);
//        accuracy = alsRating.execute();
//        end = System.currentTimeMillis();
//        results.add(" ALSRating," + (end - start) + "," + accuracy + "\r\n");

        start = System.currentTimeMillis();
        final DecisionTreeRegression dtRegression = new DecisionTreeRegression(spark);
        accuracy = dtRegression.execute();
        end = System.currentTimeMillis();
        results.add(" DecisionTreeRegression," + (end - start) + "," + accuracy + "\r\n");

        start = System.currentTimeMillis();
        final GeneralizedLinearRegression glRegression = new GeneralizedLinearRegression(spark);
        accuracy = glRegression.execute();
        end = System.currentTimeMillis();
        results.add(" GeneralizedLinearRegression," + (end - start) + "," + accuracy + "\r\n");

        start = System.currentTimeMillis();
        final RandomForestRegression rfRegression = new RandomForestRegression(spark);
        accuracy = rfRegression.execute();
        end = System.currentTimeMillis();
        results.add(" RandomForestRegression," + (end - start) + "," + accuracy + "\r\n");


        //write the output to a file
        try {
            FileWriter writer = new FileWriter("SparkMLOutput-Cluster.txt", true);
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
