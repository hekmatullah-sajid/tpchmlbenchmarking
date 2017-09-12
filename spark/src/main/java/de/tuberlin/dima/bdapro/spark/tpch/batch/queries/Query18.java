package de.tuberlin.dima.bdapro.spark.tpch.batch.queries;

import java.util.List;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import de.tuberlin.dima.bdapro.spark.tpch.config.Utils;

/**
 * Large Volume Customer Query (Q18), TPC-H Benchmark Specification page 58 http://www.tpc.org/tpc_documents_current_versions/pdf/tpc-h_v2.17.2.pdf). 
 * @author Hekmatullah Sajid
 *
 */
public class Query18 extends Query {
	
	public Query18(final SparkSession spark) {
		super(spark);
	}
	
	/**
	 * Finds the random values and passes it to the execute method (with parameter).
	 */
	@Override
	public List<Row> execute() {
		return execute(Utils.getRandomInt(312, 315));
	}
	
	/**
	 * Executes Query18 of TPC-H and returns the result.
	 * @param rndQty is randomly selected within [312..315].
	 * @return result of the query
	 */
	public List<Row> execute(int rndQty) {
		String querySQL = "SELECT c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, sum(l_quantity) from customer, orders, lineitem "
				+ "WHERE o_orderkey in (SELECT l_orderkey from lineitem group by l_orderkey having sum(l_quantity) > " + rndQty +") "
				+ "and c_custkey = o_custkey and o_orderkey = l_orderkey "
				+ "GROUP BY c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice "
				+ "ORDER BY o_totalprice desc, o_orderdate limit 100";
		
		return spark.sql(querySQL).collectAsList();
		
	}

}
