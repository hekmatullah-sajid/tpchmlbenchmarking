package de.tuberlin.dima.bdapro.flink.tpch.batch.queries;

import java.util.List;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;

import de.tuberlin.dima.bdapro.flink.tpch.batch.config.Utils;

/**
 * Customer Distribution Query (Q13), TPC-H Benchmark Specification page 51
 * http://www.tpc.org/tpc_documents_current_versions/pdf/tpc-h_v2.17.2.pdf).
 * 
 * @author Seema Narasimha Swamy
 *
 */
public class Query13 extends Query {
	public Query13(final BatchTableEnvironment env) {
		super(env);
	}

	/**
	 * Find the random values and pass it to the execute method (with
	 * parameter).
	 */
	@Override
	public List<Tuple2<Long, Long>> execute() {

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
	public List<Tuple2<Long, Long>> execute(String word1, String word2) {

		String SQLQuery = "SELECT c_count, count(*) as custdist " + "FROM ( " + "SELECT c_custkey, count(o_orderkey) "
				+ "FROM customer left outer join orders on c_custkey = o_custkey " + "and o_comment not like '%" + word1
				+ "%" + word2 + "%' " + "GROUP BY c_custkey) " + "as c_orders (c_custkey, c_count)"
				+ "GROUP BY c_count ORDER BY custdist desc, c_count desc";

		Table res = env.sql(SQLQuery);

		/*
		 * Collect and return the result.
		 */
		try {
			return env.toDataSet(res, TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {
			})).collect();
		} catch (Exception e) {
			e.printStackTrace();
		}

		return null;
	}

}
