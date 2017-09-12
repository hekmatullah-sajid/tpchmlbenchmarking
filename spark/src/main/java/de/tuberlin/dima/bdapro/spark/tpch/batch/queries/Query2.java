package de.tuberlin.dima.bdapro.spark.tpch.batch.queries;

import java.util.List;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import de.tuberlin.dima.bdapro.spark.tpch.config.Utils;
import de.tuberlin.dima.bdapro.spark.tpch.config.Utils.Nation;

/**
 * Minimum Cost Supplier Query (Q2), TPC-H Benchmark Specification page 30 http://www.tpc.org/tpc_documents_current_versions/pdf/tpc-h_v2.17.2.pdf). 
 * @author Hekmatullah Sajid
 *
 */
public class Query2 extends Query {
	
	public Query2(SparkSession spark) {
		super(spark);
	}

	/**
	 * Find the random values and pass it to the execute method (with parameter).
	 */
	@Override
	public List<Row> execute() {
		return execute(Utils.getRandomTypeSyl3(), Utils.getRandomInt(1993, 1997), Nation.getRandomRegion());
	}
	/**
	 * Executes Query2 of TPC-H and returns the result.
	 * @param pType is randomly selected within [1. 50];
	 * @param pSize is randomly selected within the list Syllable 3
	 * @param rRegion is randomly selected within the list of Regions
	 * @return result of the query
	 * 
	 */
	public List<Row> execute(String pType, int pSize, String rRegion) {
		String SQLQuery = "SELECT s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment "
		+ "FROM part, supplier, partsupp, nation, region "
		+ "WHERE p_partkey = ps_partkey and s_suppkey = ps_suppkey and p_size = " + pSize + " "
		+ "and p_type like '%" + pType + "' and s_nationkey = n_nationkey and n_regionkey = r_regionkey "
		+ "and r_name = '" + rRegion + "' "
		+ "and ps_supplycost = (SELECT min(ps_supplycost) from partsupp, supplier, nation, region "
		+ "where p_partkey = ps_partkey and s_suppkey = ps_suppkey and s_nationkey = n_nationkey and "
		+ "n_regionkey = r_regionkey and r_name = '" + rRegion + "' ) "
		+ "order by s_acctbal desc, n_name, s_name, p_partkey limit 100";
		
		return spark.sql(SQLQuery).collectAsList();
	}

}
