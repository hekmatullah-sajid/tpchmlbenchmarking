package de.tuberlin.dima.bdapro.spark.tpch.batch.queries;

import java.util.List;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import de.tuberlin.dima.bdapro.spark.tpch.config.Utils;

/**
 * Product Type Profit Measure Query (Q9), TPC-H Benchmark Specification page 43 http://www.tpc.org/tpc_documents_current_versions/pdf/tpc-h_v2.17.2.pdf). 
 * @author Hekmatullah Sajid and Seema Narasimha Swamy
 *
 */
public class Query9 extends Query {

	public Query9() {
		super();
	}

	public Query9(final SparkSession spark) {
		super(spark);
	}

	/**
	 * Find the random values and pass it to the execute method (with parameter).
	 */
	@Override
	public List<Row> execute() {
		return execute(Utils.getRandomColor());
	}

	/**
	 * Executes Query9 of TPC-H and returns the result.
	 * @param randomColor is randomly selected within the list of values defined in COLORS.
	 * @return result of the query.
	 */
	public List<Row> execute(final String randomColor) {
		return spark.sql("select nation, o_year, sum(amount) as sum_profit "
				+ "from (select n_name as nation, "
				+ "year(o_orderdate) as o_year, "
				+ "(l_extendedprice * (1 - l_discount) - (ps_supplycost * l_quantity)) as amount "
				+ "from part, supplier, lineitem, partsupp, orders, nation "
				+ "where s_suppkey = l_suppkey "
				+ "and ps_suppkey = l_suppkey "
				+ "and ps_partkey = l_partkey "
				+ "and p_partkey = l_partkey "
				+ "and o_orderkey = l_orderkey "
				+ "and s_nationkey = n_nationkey "
				+ "and p_name like '%" + randomColor + "%' "
				+ ") as profit "
				+ "group by nation, o_year "
				+ "order by nation, o_year desc").collectAsList();
	}

}
