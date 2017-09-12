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
 * Discounted Revenue Query (Q19), TPC-H Benchmark Specification page 60
 * http://www.tpc.org/tpc_documents_current_versions/pdf/tpc-h_v2.17.2.pdf).
 * 
 * @author Hekmatullah Sajid
 *
 */
public class Query19 extends Query {

	public Query19(BatchTableEnvironment env) {
		super(env);
	}

	/**
	 * Finds the random values and passes it to the execute method (with
	 * parameter).
	 */
	@Override
	public List<Tuple1<Double>> execute() {
		return execute(Utils.getRandomBrand(), Utils.getRandomBrand(), Utils.getRandomBrand(),
				Utils.getRandomInt(1, 10), Utils.getRandomInt(10, 20), Utils.getRandomInt(20, 30));
	}

	/**
	 * Executes Query19 of TPC-H and returns the result. This query has three
	 * sub-queries in WHERE clause separated by logical OR, that SQL was not
	 * supported by Flink. The result of this query is calculated by executing
	 * the outer query for each inner query and finally the union of all three
	 * queries is performed to get the final result.
	 * 
	 * @param brand1
	 *            'Brand#MN' where each MN is a two character string
	 *            representing two numbers randomly and independently selected
	 *            within [1 .. 5]
	 * @param brand2
	 *            'Brand#MN' where each MN is a two character string
	 *            representing two numbers randomly and independently selected
	 *            within [1 .. 5]
	 * @param brand3
	 *            'Brand#MN' where each MN is a two character string
	 *            representing two numbers randomly and independently selected
	 *            within [1 .. 5]
	 * @param qty1
	 *            is randomly selected within [1..10].
	 * @param qty2
	 *            is randomly selected within [10..20].
	 * @param qty3
	 *            is randomly selected within [20..30].
	 * @return result of the query
	 */
	public List<Tuple1<Double>> execute(String brand1, String brand2, String brand3, int qty1, int qty2, int qty3) {
		String querySQL1 = "SELECT sum(l_extendedprice * (1 - l_discount) ) as revenue from lineitem, part where (p_partkey = l_partkey "
				+ "and p_brand = '" + brand1 + "' and p_container in ( 'SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') "
				+ "and l_quantity >= " + qty1 + " and l_quantity <= " + (qty1 + 10) + " and p_size between 1 and 5 "
				+ "and l_shipmode in ('AIR', 'AIR REG') and l_shipinstruct = 'DELIVER IN PERSON' ) ";

		String querySQL2 = "SELECT sum(l_extendedprice * (1 - l_discount) ) as revenue from lineitem, part "
				+ "where  (p_partkey = l_partkey and p_brand = '" + brand2
				+ "' and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK') " + "and l_quantity >= " + qty2
				+ " and l_quantity <= " + (qty2 + 10) + " "
				+ "and p_size between 1 and 10 and l_shipmode in ('AIR', 'AIR REG') and l_shipinstruct = 'DELIVER IN PERSON')";
		String querySQL3 = "SELECT sum(l_extendedprice * (1 - l_discount) ) as revenue from lineitem, part "
				+ "where  (p_partkey = l_partkey and p_brand = '" + brand3
				+ "' and p_container in ( 'LG CASE', 'LG BOX', 'LG PACK', 'LG PKG') " + "and l_quantity >= " + qty3
				+ " and l_quantity <= " + (qty3 + 10) + " and p_size between 1 and 15 "
				+ "and l_shipmode in ('AIR', 'AIR REG') and l_shipinstruct = 'DELIVER IN PERSON')";

		Table res1 = env.sql(querySQL1);
		Table res2 = env.sql(querySQL2);
		Table res3 = env.sql(querySQL3);

		Table res = res1.union(res2).union(res3).select("SUM(revenue)");

		/*
		 * Drop more than two decimal values in double values. And return the
		 * result
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
}
