package de.tuberlin.dima.bdapro.flink.tpch.batch.queries;

import java.util.List;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;

import de.tuberlin.dima.bdapro.flink.tpch.Utils;
import de.tuberlin.dima.bdapro.flink.tpch.Utils.Nation;

public class Query11 extends Query {

	private double sf;

	public Query11(final BatchTableEnvironment env, final String sf) {
		super(env);
		this.sf = Double.parseDouble(sf);
	}

	@Override
	public List<Tuple2<Integer, Double>> execute() {
		return execute(Nation.getRandomNation(), 0.0001/sf);
	}

	public List<Tuple2<Integer, Double>> execute(final String nation, final double fraction) {
		Table res = env.sql(
				"SELECT ps_partkey, sum(ps_supplycost * ps_availqty) AS sorted "
						+ "FROM partsupp, supplier, nation "
						+ "WHERE ps_suppkey = s_suppkey and s_nationkey = n_nationkey "
						+ "and n_name = '" + nation + "' "
						+ "GROUP BY ps_partkey HAVING "
						+ "sum(ps_supplycost * ps_availqty) > ("
						+ "SELECT sum(ps_supplycost * ps_availqty) * " + fraction 
						+ "FROM partsupp, supplier, nation "
						+ "WHERE ps_suppkey = s_suppkey and s_nationkey = n_nationkey "
						+ "and n_name = '" + nation + "') "
						+ "ORDER BY sorted desc");

		try {
			return env.toDataSet(res, TypeInformation.of(new TypeHint<Tuple2<Integer, Double>>() {
			})).map(new MapFunction<Tuple2<Integer, Double>, Tuple2<Integer, Double>>() {
				private static final long serialVersionUID = 1L;

				@Override
				public Tuple2<Integer, Double> map(final Tuple2<Integer, Double> value) throws Exception {
					return Utils.keepOnlyTwoDecimals(value);
				}
			}).collect();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

}
