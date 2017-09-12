package de.tuberlin.dima.bdapro.flink.tpch.batch.queries;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;

import de.tuberlin.dima.bdapro.flink.tpch.batch.config.Utils;

/**
 * Global Sales Opportunity Query (Q22), TPC-H Benchmark Specification page 66
 * http://www.tpc.org/tpc_documents_current_versions/pdf/tpc-h_v2.17.2.pdf).
 * 
 * @author Seema Narasimha Swamy
 *
 */
public class Query22 extends Query {
	public Query22(BatchTableEnvironment env) {
		super(env);
	}

	/**
	 * Find the random values and pass it to the execute method (with
	 * parameter).
	 */
	@Override
	public List<Tuple3<String, Long, Double>> execute() {
		return execute(getRandomCountryCode());
	}

	/**
	 * Executes Query22 of TPC-H and returns the result.
	 * 
	 * @param countryCodes
	 *            are randomly selected without repetition from the possible
	 *            values for Country code.
	 * @return result of the query
	 */
	public List<Tuple3<String, Long, Double>> execute(List<Integer> countryCodes) {

		String SQLQuery = "SELECT cntrycode, count(*) as numcust, sum(c_acctbal) as totacctbal "
				+ "FROM ( SELECT substring(c_phone, 1, 2) as cntrycode, c_acctbal " + "FROM customer "
				+ "WHERE substring(c_phone, 1, 2) in " + "('" + countryCodes.get(0) + "', ' " + countryCodes.get(1)
				+ "', '" + countryCodes.get(2) + "', '" + countryCodes.get(3) + "', '" + countryCodes.get(4) + "', '"
				+ countryCodes.get(5) + "', '" + countryCodes.get(6) + "' ) "
				+ "AND c_acctbal > ( SELECT avg(c_acctbal) FROM customer " + "WHERE c_acctbal > 0.00 "
				+ "AND substring (c_phone, 1, 2) in " + "(' " + countryCodes.get(0) + "', ' " + countryCodes.get(1)
				+ "', '" + countryCodes.get(2) + "', ' " + countryCodes.get(3) + "', '" + countryCodes.get(4) + "', '"
				+ countryCodes.get(5) + "', '" + countryCodes.get(6) + "') ) " + "AND NOT EXISTS ( "
				+ "SELECT * FROM orders WHERE o_custkey = c_custkey " + ") ) as custsale "
				+ "GROUP BY cntrycode ORDER BY cntrycode";

		Table res = env.sql(SQLQuery);

		/*
		 * Drop more than two decimal values in double values. And return the
		 * result.
		 */
		try {
			return env.toDataSet(res, TypeInformation.of(new TypeHint<Tuple3<String, Long, Double>>() {
			})).map(new MapFunction<Tuple3<String, Long, Double>, Tuple3<String, Long, Double>>() {
				private static final long serialVersionUID = 1L;

				@Override
				public Tuple3<String, Long, Double> map(final Tuple3<String, Long, Double> value) throws Exception {
					return Utils.keepOnlyTwoDecimals(value);
				}
			}).collect();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * Query 22 of TPC-H Benchmark needs a substitution parameter that must be
	 * generated and used to build the executable query text. The substitution
	 * parameter is Country codes that are randomly selected without repetition
	 * from the possible values for Country code.
	 * 
	 * @return a list of unique and randomly selected country codes
	 */
	private List<Integer> getRandomCountryCode() {
		Set<Integer> countrycodes = new LinkedHashSet<>();
		while (countrycodes.size() < 7) {
			Integer next = Utils.getRandomInt(0, 24) + 10;
			// As we're adding to a set, this will automatically do a
			// containment check
			countrycodes.add(next);
		}
		return new ArrayList<>(countrycodes);
	}
}
