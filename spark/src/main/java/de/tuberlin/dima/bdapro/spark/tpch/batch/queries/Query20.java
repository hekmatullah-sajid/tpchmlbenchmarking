package de.tuberlin.dima.bdapro.spark.tpch.batch.queries;

import java.time.LocalDate;
import java.util.List;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import de.tuberlin.dima.bdapro.spark.tpch.config.Utils;
import de.tuberlin.dima.bdapro.spark.tpch.config.Utils.Nation;

/**
 * Potential Part Promotion Query (Q20), TPC-H Benchmark Specification page 62 http://www.tpc.org/tpc_documents_current_versions/pdf/tpc-h_v2.17.2.pdf). 
 * @author Hekmatullah Sajid
 *
 */
public class Query20 extends Query {
	
	public Query20(SparkSession spark) {
		super(spark);
	}

	/**
	 * Finds the random values and passes it to the execute method (with parameter).
	 */
	@Override
	public List<Row> execute() {
		return execute(Utils.getRandomColor(), LocalDate.parse(Utils.getRandomInt(1993, 1997) + "-01-01"), Nation.getRandomNation());
	}
	
	/**
	 * 
	 * Executes Query20 of TPC-H and returns the result.
	 * 
	 * @param rndColor is randomly selected within the COLORS list of values.
	 * @param rndDate is the first of January of a randomly selected year within 1993-1997.
	 * @param rndNation is randomly selected within the list of Nation
	 * @return result of the query
	 */
	public List<Row> execute(String rndColor, LocalDate rndDate, String rndNation) {
		String querySQL = "SELECT s_name, s_address FROM supplier, nation WHERE s_suppkey in ("
				+ "SELECT ps_suppkey FROM partsupp WHERE ps_partkey IN (SELECT p_partkey FROM part WHERE p_name LIKE '" + rndColor + "%') "
				+ "and ps_availqty > ( SELECT 0.5 * SUM(l_quantity) FROM lineitem WHERE l_partkey = ps_partkey and l_suppkey = ps_suppkey "
				+ "and l_shipdate >= '" + rndDate.toString() + "' and l_shipdate < '" + rndDate.plusYears(1).toString() + "' ) ) and s_nationkey = n_nationkey "
				+ "and n_name = '" + rndNation +"' ORDER BY s_name";
		
		return spark.sql(querySQL).collectAsList();
	}

}
