package de.tuberlin.dima.bdapro.spark.tpch.batch.queries;

import java.util.List;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public abstract class Query {

	protected SparkSession spark;

	public Query(){}

	public Query(final SparkSession spark) {
		this.spark = spark;
	}

	public abstract List<Row> execute();
}
