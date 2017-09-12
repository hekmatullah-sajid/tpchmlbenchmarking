package de.tuberlin.dima.bdapro.spark.tpch.batch.queries;

import java.time.LocalDate;
import java.util.List;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import de.tuberlin.dima.bdapro.spark.tpch.config.Utils;
import de.tuberlin.dima.bdapro.spark.tpch.config.Utils.Nation;

/**
 * Local Supplier Volume Query (Q5), TPC-H Benchmark Specification page 36 http://www.tpc.org/tpc_documents_current_versions/pdf/tpc-h_v2.17.2.pdf). 
 * @author Hekmatullah Sajid
 *
 */
public class Query5 extends Query {
	
	public Query5(final SparkSession spark) {
		super(spark);
	}

	/**
	 * Finds the random values and passes it to the execute method (with parameter).
	 */
	@Override
	public List<Row> execute() {
		return execute(Nation.getRandomRegion(), LocalDate.parse(Utils.getRandomInt(1993, 1997) + "-01-01"));
	}
	
	/**
	 * Executes Query5 of TPC-H and returns the result.
	 * @param rndRegion is randomly selected within the list of Regions
	 * @param rndDate is the first of January of a randomly selected year within [1993 .. 1997].
	 * @return result of the query
	 */
	public List<Row> execute(String rndRegion, LocalDate rndDate) {
		
		String SQLQuery = "SELECT n_name, sum(l_extendedprice * (1 - l_discount)) as revenue "
				+ "FROM customer, orders, lineitem, supplier, nation, region "
				+ "WHERE c_custkey = o_custkey and l_orderkey = o_orderkey "
				+ "and l_suppkey = s_suppkey and c_nationkey = s_nationkey and s_nationkey = n_nationkey and n_regionkey = r_regionkey "
				+ "and r_name = '" + rndRegion +"' and "
				+ "o_orderdate >= '" + rndDate.toString() +"' and "
				+ "o_orderdate < '" + rndDate.plusYears(1).toString() + "' " 
				+ "GROUP BY n_name ORDER BY revenue desc";
		
		return spark.sql(SQLQuery).collectAsList();
	}

}
