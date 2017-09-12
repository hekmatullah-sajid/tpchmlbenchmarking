package de.tuberlin.dima.bdapro.flink.tpch.batch.queries;

import java.util.List;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;

import de.tuberlin.dima.bdapro.flink.tpch.batch.config.Utils;

/**
 * Product Type Profit Measure Query (Q9), TPC-H Benchmark Specification page 43
 * http://www.tpc.org/tpc_documents_current_versions/pdf/tpc-h_v2.17.2.pdf).
 * 
 * @author Hekmatullah Sajid and Seema Narasimha Swamy
 *
 */
public class Query9 extends Query {

	public Query9(final BatchTableEnvironment env) {
		super(env);
	}

	/**
	 * Find the random values and pass it to the execute method (with
	 * parameter).
	 */
	@Override
	public List<Tuple3<String, Long, Double>> execute() {
		return execute(Utils.getRandomColor());
	}

	/**
	 * Executes Query9 of TPC-H and returns the result. The SQL of the query was
	 * not supported by Flink, the query is excuted using the Table API from
	 * Flink.
	 * 
	 * @param randomColor
	 *            is randomly selected within the list of values defined in
	 *            COLORS.
	 * @return result of the query.
	 */
	public List<Tuple3<String, Long, Double>> execute(final String color) {

		Table lineitem = env.scan("lineitem");
		Table part = env.scan("part").filter("LIKE(p_name,'%" + color + "%')");
		Table supplier = env.scan("supplier");
		Table orders = env.scan("orders");
		Table nation = env.scan("nation");
		Table partsupp = env.scan("partsupp");

		Table innerRes = supplier.join(lineitem).where("s_suppkey = l_suppkey").join(partsupp)
				.where("l_suppkey = ps_suppkey").where("l_partkey = ps_partkey").join(part)
				.where("p_partkey = l_partkey").join(orders).where("l_orderkey = o_orderkey").join(nation)
				.where("s_nationkey = n_nationkey")
				.select("n_name as nation, " + "o_orderdate.toDate.extract(YEAR) as o_year, "
						+ "l_extendedprice*(1-l_discount)-ps_supplycost*l_quantity as amount");

		Table res = innerRes.groupBy("nation, o_year").select("nation, o_year, sum(amount) as sum_profit")
				.orderBy("nation, o_year.desc");

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

}
