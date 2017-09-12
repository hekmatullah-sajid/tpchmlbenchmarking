package de.tuberlin.dima.bdapro.flink.tpch.batch.queries;

import java.time.LocalDate;
import java.util.List;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;

import de.tuberlin.dima.bdapro.flink.tpch.batch.config.Utils;

/**
 * Promotion Effect Query (Q14), TPC-H Benchmark Specification page 52
 * http://www.tpc.org/tpc_documents_current_versions/pdf/tpc-h_v2.17.2.pdf).
 * 
 * @author Seema Narasimha Swamy
 *
 */
public class Query14 extends Query {
	public Query14(final BatchTableEnvironment env) {
		super(env);
	}

	/**
	 * Find the random values and pass it to the execute method (with
	 * parameter).
	 */
	@Override
	public List<Tuple1<Double>> execute() {

		return execute(getRandomDate());
	}

	/**
	 * Executes Query14 of TPC-H and returns the result.
	 * 
	 * @param date
	 *            is the first day of a month randomly selected from a random
	 *            year within [1993 .. 1997].
	 * @return result of the query
	 */
	public List<Tuple1<Double>> execute(LocalDate date) {
		String SQLQuery = "SELECT 100.00 * sum(case " + "WHEN p_type like 'PROMO%' "
				+ "THEN l_extendedprice*(1-l_discount) " + "ELSE 0 "
				+ "end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue " + "FROM lineitem,part "
				+ "WHERE l_partkey = p_partkey " + "and l_shipdate >=  '" + date.toString() + "' "
				+ "and l_shipdate < '" + date.plusMonths(1).toString() + "' ";

		Table res = env.sql(SQLQuery);

		/*
		 * Drop more than two decimal values in double values. And return the
		 * result.
		 */
		try {
			return env.toDataSet(res, TypeInformation.of(new TypeHint<Tuple1<Double>>() {
			})).map(new MapFunction<Tuple1<Double>, Tuple1<Double>>() {
				private static final long serialVersionUID = 1L;

				@Override
				public Tuple1<Double> map(final Tuple1<Double> value) throws Exception {
					return Utils.keepOnlyTwoDecimals(value);
				}
			}).collect();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * Get the substitution parameter date, selected as the first day of a month
	 * randomly selected from a random year within [1993 .. 1997].
	 * 
	 * @return array of randomly selected nations
	 */
	private LocalDate getRandomDate() {
		return LocalDate.of(Utils.getRandomInt(1992, 1997), Utils.getRandomInt(1, 12), 1);
	}
}
