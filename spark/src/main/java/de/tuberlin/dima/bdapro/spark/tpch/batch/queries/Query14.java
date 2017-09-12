package de.tuberlin.dima.bdapro.spark.tpch.batch.queries;

import java.time.LocalDate;
import java.util.List;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import de.tuberlin.dima.bdapro.spark.tpch.config.Utils;

/**
 * Promotion Effect Query (Q14), TPC-H Benchmark Specification page 52
 * http://www.tpc.org/tpc_documents_current_versions/pdf/tpc-h_v2.17.2.pdf).
 * 
 * @author Seema Narasimha Swamy
 *
 */
public class Query14 extends Query {
	public Query14(SparkSession spark) {
		super(spark);
	}

	/**
	 * Find the random values and pass it to the execute method (with parameter).
	 * 
	 */
	@Override
	public List<Row> execute() {
		return execute(getRandomDate());
	}

	/**
	 * Executes Query14 of TPC-H and returns the result.
	 * 
	 * @param date
	 *            is the first day of a month randomly selected from a random
	 *            year within [1993 .. 1997].
	 * @return result of the query
	 */
	public List<Row> execute(LocalDate date) {
		String querySQL = "SELECT 100.00 * sum(case " + "WHEN p_type like 'PROMO%' "
				+ "THEN l_extendedprice*(1-l_discount) " + "ELSE 0 "
				+ "end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue " + "FROM lineitem,part "
				+ "WHERE l_partkey = p_partkey " + "and l_shipdate >=  '" + date.toString() + "' "
				+ "and l_shipdate < '" + date.plusMonths(1).toString() + "' ";
		return spark.sql(querySQL).collectAsList();
	}

	/**
	 * Get the substitution parameter date, selected as the first day of a month randomly selected from a random year within [1993 .. 1997].
	 * @return array of randomly selected nations
	 */
	private LocalDate getRandomDate() {
		return LocalDate.of(Utils.getRandomInt(1992, 1997), Utils.getRandomInt(0, 12), 1);
	}
}
