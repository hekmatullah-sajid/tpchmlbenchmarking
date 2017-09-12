package de.tuberlin.dima.bdapro.flink.tpch.batch.queries;

import java.util.List;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;

import de.tuberlin.dima.bdapro.flink.tpch.batch.config.Utils;

/**
 * Forecasting Revenue Change Query (Q6), TPC-H Benchmark Specification page 38
 * http://www.tpc.org/tpc_documents_current_versions/pdf/tpc-h_v2.17.2.pdf).
 * 
 * @author Hekmatullah Sajid and Seema Narasimha Swamy
 *
 */
public class Query6 extends Query {

	public Query6(final BatchTableEnvironment env) {
		super(env);
	}

	/**
	 * Find the random values and pass it to the execute method (with
	 * parameter).
	 */
	@Override
	public List<Tuple1<Double>> execute() {
		return execute(Utils.getRandomInt(1993, 1997) + "-01-01", Utils.getRandomDouble(0.02, 0.09),
				Utils.getRandomInt(24, 25));
	}

	/**
	 * Executes Query6 of TPC-H and returns the result. Flink was not able to
	 * execute the query SQL, instead the Table API is used to get the result of
	 * the query.
	 * 
	 * @param date
	 *            is the first of January of a randomly selected year within
	 *            [1993 .. 1997];
	 * @param discount
	 *            is randomly selected within [0.02 .. 0.09];
	 * @param quantity
	 *            is randomly selected within [24 .. 25].
	 * @return result of the query
	 */
	public List<Tuple1<Double>> execute(final String date, final double discount, final int quantity) {
		Table lineitem = env.scan("lineitem");

		double lowerBoundDiscount = Utils.convertToTwoDecimal(discount - 0.01);
		double upperBoundDiscount = Utils.convertToTwoDecimal(discount + 0.01);

		Table result = lineitem.where("l_shipdate.toDate >= '" + date + "'.toDate ")
				.where("l_shipdate.toDate < ('" + date + "'.toDate + 1.year) ")
				.where("l_discount >= " + lowerBoundDiscount).where("l_discount <= " + upperBoundDiscount)
				.where("l_quantity < " + quantity).select("sum(l_extendedprice*l_discount) as revenue");

		/*
		 * Drop more than two decimal values in double values. And return the
		 * result.
		 */

		try {
			return env.toDataSet(result, TypeInformation.of(new TypeHint<Tuple1<Double>>() {
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

}
