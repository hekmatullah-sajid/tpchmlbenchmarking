package de.tuberlin.dima.bdapro.flink.tpch.batch.queries;

import java.util.List;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

import de.tuberlin.dima.bdapro.flink.tpch.Utils;
import de.tuberlin.dima.bdapro.flink.tpch.Utils.Nation;

public class Query8 extends Query {

	public Query8(final BatchTableEnvironment env) {
		super(env);
	}

	@Override
	public List<Tuple2<Long, Double>> execute() {
		Nation nation = Nation.getRandomNationAndRegion();
		return execute(nation.getName(), nation.getRegion(), Utils.getRandomType());
	}

	public List<Tuple2<Long, Double>> execute(final String nation, final String region, final String type) {
		// register the function
		env.registerFunction("volumeFilter", new VolumeFilter());

		Table lineitem = env.scan("lineitem");
		Table part = env.scan("part")
				.filter("p_type = '" + type + "'");
		Table supplier = env.scan("supplier");
		Table orders = env.scan("orders")
				.filter("o_orderdate.toDate >= '1995-01-01'.toDate")
				.filter("o_orderdate.toDate <= '1996-12-31'.toDate");
		Table customer = env.scan("customer");
		Table nation1Table = env.scan("nation")
				.as("n1_nationkey, n1_name, n1_regionkey, n1_comment");
		Table nation2Table = env.scan("nation")
				.as("n2_nationkey, n2_name, n2_regionkey, n2_comment");
		Table regionTable = env.scan("region")
				.filter("r_name = '" + region + "'");

		Table innerRes = part.join(lineitem).where("p_partkey = l_partkey")
				.join(supplier).where("l_suppkey = s_suppkey")
				.join(orders).where("l_orderkey = o_orderkey")
				.join(customer).where("o_custkey = c_custkey")
				.join(nation1Table).where("c_nationkey = n1_nationkey")
				.join(regionTable).where("n1_regionkey = r_regionkey")
				.join(nation2Table).where("s_nationkey = n2_nationkey")
				.select("o_orderdate.toDate.extract(YEAR) as o_year, "
						+ "(l_extendedprice*(1-l_discount)) as volume, "
						+ "n2_name as nation");

		Table res = innerRes.groupBy("o_year")
				.select("o_year, (sum(volumeFilter(volume,nation,'" + nation + "'))/sum(volume)) as mkt_share")
				.orderBy("o_year");

		try {
			return env.toDataSet(res, TypeInformation.of(new TypeHint<Tuple2<Long, Double>>() {
			})).map(new MapFunction<Tuple2<Long, Double>, Tuple2<Long, Double>>() {
				private static final long serialVersionUID = 1L;

				@Override
				public Tuple2<Long, Double> map(final Tuple2<Long, Double> value) throws Exception {
					return Utils.keepOnlyTwoDecimals(value);
				}
			}).collect();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	public static class VolumeFilter extends ScalarFunction {
		public double eval(final Double volume, final String nation, final String nation2) {
			if(nation.equals(nation2)) {
				return volume;
			}
			return 0;
		}

		@Override
		public TypeInformation<?> getResultType(final Class<?>[] signature) {
			return Types.DOUBLE();
		}
	}

}
