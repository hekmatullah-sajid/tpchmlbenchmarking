package de.tuberlin.dima.bdapro.spark.tpch.batch.queries;

import java.util.List;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import de.tuberlin.dima.bdapro.spark.tpch.config.Utils.Nation;

/**
 * Suppliers Who Kept Orders Waiting Query (Q21), TPC-H Benchmark Specification page 64 http://www.tpc.org/tpc_documents_current_versions/pdf/tpc-h_v2.17.2.pdf). 
 * @author Hekmatullah Sajid
 *
 */
public class Query21 extends Query {
	
	public Query21(SparkSession spark) {
		super(spark);
	}

	/**
	 * Finds the random values and passes it to the execute method (with parameter).
	 */
	@Override
	public List<Row> execute() {
		return execute(Nation.getRandomNation());
	}
	
	/**
	 * 
	 * Executes Query21 of TPC-H and returns the result.
	 * @param rndNation is randomly selected within the list of Nation
	 * @return result of the query
	 */
	public List<Row> execute(String rndNation) {
		String querySQL = "SELECT s_name, count(*) as numwait from supplier, lineitem l1, orders, nation where s_suppkey = l1.l_suppkey "
				+ "and o_orderkey = l1.l_orderkey and o_orderstatus = 'F' and l1.l_receiptdate > l1.l_commitdate and exists ( "
				+ "select * from lineitem l2 where l2.l_orderkey = l1.l_orderkey and l2.l_suppkey <> l1.l_suppkey ) "
				+ "and not exists (select * from lineitem l3 where l3.l_orderkey = l1.l_orderkey and l3.l_suppkey <> l1.l_suppkey "
				+ "and l3.l_receiptdate > l3.l_commitdate ) and s_nationkey = n_nationkey and n_name = '" + rndNation + "' group by s_name "
				+ "order by numwait desc, s_name limit 100";
		
		return spark.sql(querySQL).collectAsList();
	}

}
