package de.tuberlin.dima.bdapro.sparkml;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;

/**
 * Created by seema on 15.08.17.
 */
public abstract class MLAlgorithmBase {
    protected SparkSession spark;
    //protected Dataset<Row> data;

    public MLAlgorithmBase(){}

    public MLAlgorithmBase(final SparkSession spark) {
        this.spark = spark;
    }

    public abstract void execute();



}
