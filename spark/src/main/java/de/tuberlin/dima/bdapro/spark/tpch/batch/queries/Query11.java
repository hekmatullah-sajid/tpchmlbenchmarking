package de.tuberlin.dima.bdapro.spark.tpch.batch.queries;

import java.util.List;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import de.tuberlin.dima.bdapro.spark.tpch.config.Utils.Nation;

/**
 * Important Stock Identification Query (Q11), TPC-H Benchmark Specification page 47 http://www.tpc.org/tpc_documents_current_versions/pdf/tpc-h_v2.17.2.pdf). 
 * @author Hekmatullah Sajid and Seema Narasimha Swamy
 *
 */
public class Query11 extends Query {

	/*
	 * Scale Factor is used as a substitution parameter to build the executable query text.
	 */
	private double sf;

	public Query11(final SparkSession spark, final String sf) {
		super(spark);
		this.sf = Double.parseDouble(sf);
	}

	/**
	 * Find the random values and pass it to the execute method (with parameter).
	 */
	@Override
	public List<Row> execute() {
		return execute(Nation.getRandomNation(), 0.0001/sf);
	}

	/**
	 * Executes Query11 of TPC-H and returns the result.
	 * @param randomNation is randomly selected within the list of Nation
	 * @param fraction is chosen as 0.0001 / SF.
	 * @return result of the query
	 */
	public List<Row> execute(final String randomNation, final double fraction) {
		return spark.sql(
				"SELECT ps_partkey, sum(ps_supplycost * ps_availqty) AS sorted "
						+ "FROM partsupp, supplier, nation "
						+ "WHERE ps_suppkey = s_suppkey and s_nationkey = n_nationkey "
						+ "and n_name = '" + randomNation + "' "
						+ "GROUP BY ps_partkey HAVING "
						+ "sum(ps_supplycost * ps_availqty) > ("
						+ "SELECT sum(ps_supplycost * ps_availqty) * " + fraction 
						+ " FROM partsupp, supplier, nation "
						+ "WHERE ps_suppkey = s_suppkey and s_nationkey = n_nationkey "
						+ "and n_name = '" + randomNation + "') "
						+ "ORDER BY sorted desc").collectAsList();
	}

}
