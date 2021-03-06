package de.tuberlin.dima.bdapro.spark.tpch.batch.queries;

import java.util.List;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import de.tuberlin.dima.bdapro.spark.tpch.config.Utils;

/**
 * Customer Distribution Query (Q13), TPC-H Benchmark Specification page 51 http://www.tpc.org/tpc_documents_current_versions/pdf/tpc-h_v2.17.2.pdf). 
 * @author Seema Narasimha Swamy
 *
 */
public class Query13 extends Query {
	public Query13(SparkSession spark) {
		super(spark);
	}

	/**
	 * Find the random values and pass it to the execute method (with parameter).
	 */
	@Override
	public List<Row> execute() {
		return execute(Utils.getRandomWord1(), Utils.getRandomWord2());
	}

	/**
	 * Executes Query13 of TPC-H and returns the result.
	 * 
	 * @param word1
	 *            is randomly selected from 4 possible values in the list
	 *            WORD_1.
	 * @param word2
	 *            is randomly selected from 4 possible values in the list
	 *            WORD_2.
	 * @return result of the query
	 */
	public List<Row> execute(String word1, String word2) {
		return spark.sql(
				"SELECT c_count, count(*) as custdist " + "FROM ( " + "SELECT c_custkey, count(o_orderkey) as c_count "
						+ "FROM customer left outer join orders on c_custkey = o_custkey " + "and o_comment not like '%"
						+ word1 + "%" + word2 + "%' " + "GROUP BY c_custkey) "
						+ "GROUP BY c_count ORDER BY custdist desc, c_count desc ")
				.collectAsList();
	}

}
