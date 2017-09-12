package de.tuberlin.dima.bdapro.flink.tpch.batch.queries;

import java.time.LocalDate;
import java.util.List;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;

import de.tuberlin.dima.bdapro.flink.tpch.batch.config.Utils;

/**
 * Top Supplier Query (Q15), TPC-H Benchmark Specification page 53
 * http://www.tpc.org/tpc_documents_current_versions/pdf/tpc-h_v2.17.2.pdf).
 * 
 * @author Hekmatullah Sajid
 *
 */
public class Query15 extends Query {

	public Query15(BatchTableEnvironment env) {
		super(env);
	}

	/**
	 * Finds the random values and passes it to the execute method (with
	 * parameter).
	 */
	@Override
	public List<Tuple5<Integer, String, String, String, Double>> execute() {
		return execute(LocalDate.parse(Utils.getRandomInt(1993, 1997) + "-01-01"));
	}

	/**
	 * Query15 first creates a view and then performs the query SQL on the
	 * created view and supplier table. The view is not supported, first Flink
	 * Table is created and registered in BatchTableEnvironment. Finally the
	 * query SQL is executed on the supplier table and the table created for
	 * view to get the query result.
	 * 
	 * @param rndDate
	 *            is the first day of a randomly selected month between the
	 *            first month of 1993 and the 10th month of 1997.
	 * @return result of the query
	 */
	public List<Tuple5<Integer, String, String, String, Double>> execute(final LocalDate rndDate) {
		String viewSQL = "SELECT l_suppkey as supplier_no, SUM(l_extendedprice * (1 - l_discount)) as total_revenue FROM lineitem WHERE l_shipdate >= '"
				+ rndDate.toString() + "' " + "and l_shipdate < '" + rndDate.plusMonths(3).toString() + "' "
				+ "GROUP BY l_suppkey";
		Table viewRevenue = env.sql(viewSQL);

		env.registerTable("viewrevenue", viewRevenue);

		String resSQL = "SELECT s_suppkey, s_name, s_address, s_phone, total_revenue FROM supplier, viewrevenue "
				+ "WHERE s_suppkey = supplier_no and total_revenue = ( "
				+ "SELECT max(total_revenue) from viewrevenue ) ORDER BY s_suppkey";

		Table res = env.sql(resSQL);

		/*
		 * Drop more than two decimal values in double values. And return the
		 * result
		 */
		try {
			return env
					.toDataSet(res, TypeInformation.of(new TypeHint<Tuple5<Integer, String, String, String, Double>>() {
					}))
					.map(new MapFunction<Tuple5<Integer, String, String, String, Double>, Tuple5<Integer, String, String, String, Double>>() {
						private static final long serialVersionUID = 1L;

						@Override
						public Tuple5<Integer, String, String, String, Double> map(
								final Tuple5<Integer, String, String, String, Double> value) throws Exception {
							return Utils.keepOnlyTwoDecimals(value);
						}
					}).collect();
		} catch (Exception e) {
			e.printStackTrace();
		}

		return null;
	}

}
