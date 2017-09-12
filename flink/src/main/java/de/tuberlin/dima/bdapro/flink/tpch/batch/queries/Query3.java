package de.tuberlin.dima.bdapro.flink.tpch.batch.queries;

import java.time.LocalDate;
import java.util.List;
import java.util.Random;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;

import de.tuberlin.dima.bdapro.flink.tpch.batch.config.Utils;

/**
 * Shipping Priority Query (Q3), TPC-H Benchmark Specification page 33
 * http://www.tpc.org/tpc_documents_current_versions/pdf/tpc-h_v2.17.2.pdf).
 * 
 * @author Seema Narasimha Swamy
 *
 */
public class Query3 extends Query {
	public Query3(final BatchTableEnvironment env) {
		super(env);
	}

	/**
	 * Find the random values and pass it to the execute method (with
	 * parameter).
	 */
	@Override
	public List<Tuple4<Integer, Double, String, Integer>> execute() {
		String segment = Utils.getRandomSegment();
		return execute(segment, getRandomDate());
	}

	/**
	 * Executes Query3 of TPC-H and returns the result.
	 * 
	 * @param segment
	 *            is randomly selected within the SEGMENTS list of values
	 * @param date
	 *            is a randomly selected day within [1995-03-01 .. 1995-03-31].
	 * @return result of the query
	 */
	public List<Tuple4<Integer, Double, String, Integer>> execute(String segment, LocalDate date) {
		String SQLQuery = "select l_orderkey, sum(l_extendedprice * (1-l_discount)) as revenue, o_orderdate, o_shipriority "
				+ "FROM customer, orders, lineitem " + "WHERE c_mktsegment = '" + segment + "' and "
				+ "c_custkey = o_custkey and  l_orderkey = o_orderkey and " + "o_orderdate < '" + date.toString()
				+ "' and " + "l_shipdate > '" + date.toString() + "' "
				+ "GROUP BY l_orderkey, o_orderdate, o_shipriority ORDER BY revenue desc, o_orderdate";

		Table res = env.sql(SQLQuery);

		/*
		 * Drop more than two decimal values in double values. And return the
		 * result
		 */
		try {
			return env.toDataSet(res, TypeInformation.of(new TypeHint<Tuple4<Integer, Double, String, Integer>>() {
			})).map(new MapFunction<Tuple4<Integer, Double, String, Integer>, Tuple4<Integer, Double, String, Integer>>() {
				private static final long serialVersionUID = 1L;

				@Override
				public Tuple4<Integer, Double, String, Integer> map(
						final Tuple4<Integer, Double, String, Integer> value) throws Exception {
					return Utils.keepOnlyTwoDecimals(value);
				}
			}).collect();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * Get the substitution parameter Date, randomly selected day within
	 * [1995-03-01 .. 1995-03-31].
	 * 
	 * @return array of randomly selected nations
	 */
	private LocalDate getRandomDate() {
		Random rand = new Random();
		return LocalDate.of(1995, 3, rand.nextInt((31 - 1) + 1) + 1);
	}
}
