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
 * Small-Quantity-Order Revenue Query (Q17), TPC-H Benchmark Specification page
 * 57 http://www.tpc.org/tpc_documents_current_versions/pdf/tpc-h_v2.17.2.pdf).
 * 
 * @author Seema Narasimha Swamy
 *
 */
public class Query17 extends Query {
	public Query17(BatchTableEnvironment env) {
		super(env);
	}

	/**
	 * Find the random values and pass it to the execute method (with
	 * parameter).
	 */
	@Override
	public List<Tuple1<Double>> execute() {
		return execute(Utils.getRandomBrand(), Utils.getRandomContainer());
	}

	/**
	 * Executes Query17 of TPC-H and returns the result.
	 * 
	 * @param brand
	 *            'Brand#MN' where MN is a two character string representing two
	 *            numbers randomly and independently selected within [1 .. 5] by
	 *            respectively utility function;
	 * @param container
	 *            is randomly selected within the lists of CONTAINERS_SYL1 and
	 *            CONTAINERS_SYL2
	 * @return the result of the query
	 */
	public List<Tuple1<Double>> execute(String brand, String container) {
		String SQLQuery = "SELECT sum(l_extendedprice) / 7.0 as avg_yearly " + "FROM lineitem, part "
				+ "WHERE p_partkey = l_partkey and p_brand = '" + brand + "' " + "and p_container = '" + container
				+ "' " + "and l_quantity < ( " + "select 0.2 * avg(l_quantity) from lineitem "
				+ "where l_partkey = p_partkey ) ";

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
}
