package de.tuberlin.dima.bdapro.spark.tpch.batch.queries;

import java.time.LocalDate;
import java.util.List;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import de.tuberlin.dima.bdapro.spark.tpch.config.Utils;

/**
 * Forecasting Revenue Change Query (Q6), TPC-H Benchmark Specification page 38 http://www.tpc.org/tpc_documents_current_versions/pdf/tpc-h_v2.17.2.pdf). 
 * @author Hekmatullah Sajid and Seema Narasimha Swamy
 *
 */
public class Query6 extends Query{

	public Query6(final SparkSession spark) {
		super(spark);
	}

	/**
	 * Find the random values and pass it to the execute method (with parameter).
	 */
	@Override
	public List<Row> execute() {
		return execute(Utils.getRandomInt(1993, 1997) + "-01-01", Utils.getRandomDouble(0.02, 0.09), Utils.getRandomInt(24, 25));
	}

	/**
	 * Executes Query6 of TPC-H and returns the result.
	 * @param date is the first of January of a randomly selected year within [1993 .. 1997];
	 * @param discount is randomly selected within [0.02 .. 0.09];
	 * @param quantity is randomly selected within [24 .. 25].
	 * @return result of the query
	 */
	public List<Row> execute(final String date, final double discount, final int quantity) {
		LocalDate dateRandom = LocalDate.parse("1994-01-01");
		LocalDate interval = dateRandom.plusYears(1);

		return spark.sql("select sum(l_extendedprice*l_discount) as revenue "
				+ "from lineitem "
				+ "where l_shipdate >= '" + dateRandom.toString() + "' "
				+ "and l_shipdate < '" + interval.toString() + "' "
				+ "and l_discount between 0.05 and 0.07 "
				+ "and l_quantity < 24").collectAsList();
	}

}
