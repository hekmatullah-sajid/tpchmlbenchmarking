package de.tuberlin.dima.bdapro.spark.tpch.batch.queries;

import java.util.List;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * The blueprint class for all queries which they extends this class.
 * @author Hekmatullah Sajid and Seema Narasimha Swamy
 *
 */
public abstract class Query {

	protected SparkSession spark;

	public Query(){}

	public Query(final SparkSession spark) {
		this.spark = spark;
	}
	
	/**
	 * All query classes implement the execute blueprint abstract execute method to execute the TPCH SQL query.
	 * This method generates substitution parameters that must be generated and used to build the executable query text. 
	 * The override execute method than calls the query's execute method (with parameters) and passes the random values.
	 */
	public abstract List<Row> execute();
}
