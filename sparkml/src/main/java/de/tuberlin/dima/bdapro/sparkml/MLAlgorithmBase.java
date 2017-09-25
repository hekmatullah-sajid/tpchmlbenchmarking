package de.tuberlin.dima.bdapro.sparkml;

import org.apache.spark.sql.SparkSession;

/**
 * The blueprint class for all algorithms to extend this class.
 * @author Seema Narasimha Swamy
 *
 */
public abstract class MLAlgorithmBase {
    protected SparkSession spark;

    public MLAlgorithmBase(){}

    public MLAlgorithmBase(final SparkSession spark) {
        this.spark = spark;
    }

    public abstract double execute();



}
