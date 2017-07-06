package de.tuberlin.dima.bdapro.flink.tpch.batch.queries;

import java.util.List;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;

import de.tuberlin.dima.bdapro.flink.tpch.Utils;
import de.tuberlin.dima.bdapro.flink.tpch.Utils.Nation;

public class Query7 extends Query {

	public Query7(final BatchTableEnvironment env) {
		super(env);
	}

	@Override
	public List<Tuple4<String, String, Long, Double>> execute() {
		String[] randNations = getTwoRandomNations();
		return execute(randNations[0], randNations[1]);
	}

	public List<Tuple4<String, String, Long, Double>> execute(final String nation1, final String nation2) {
		Table lineitem = env.scan("lineitem")
				.filter("(l_shipdate).toDate >= '1995-01-01'.toDate")
				.filter("(l_shipdate).toDate <= '1996-12-31'.toDate");
		Table supplier = env.scan("supplier");
		Table orders = env.scan("orders");
		Table customer = env.scan("customer");
		Table nation1Table = env.scan("nation")
				.as("n1_nationkey, n1_name, n1_regionkey, n1_comment")
				.filter("n1_name = '" + nation1 + "'");
		Table nation2Table = env.scan("nation")
				.as("n2_nationkey, n2_name, n2_regionkey, n2_comment")
				.filter("n2_name = '" + nation2 + "'");

		Table innerRes = supplier.join(nation1Table).where("s_nationkey = n1_nationkey").join(lineitem)
				.where("s_suppkey = l_suppkey").join(orders).where("o_orderkey = l_orderkey")
				.join(customer).where("c_custkey = o_custkey").join(nation2Table)
				.where("c_nationkey = n2_nationkey")
				.select("n1_name as supp_nation, n2_name as cust_nation, l_shipdate.toDate().extract(YEAR) as l_year, "
						+ "(l_extendedprice*(1-l_discount)) as volume");

		Table res = innerRes.groupBy("supp_nation, cust_nation, l_year")
				.select("supp_nation, cust_nation, l_year, sum(volume) as revenue")
				.orderBy("supp_nation, cust_nation, l_year");

		try {
			return env.toDataSet(res, TypeInformation.of(new TypeHint<Tuple4<String, String, Long, Double>>() {
			})).map(new MapFunction<Tuple4<String, String, Long, Double>, Tuple4<String, String, Long, Double>>() {
				private static final long serialVersionUID = 1L;

				@Override
				public Tuple4<String, String, Long, Double> map(final Tuple4<String, String, Long, Double> value) throws Exception {
					return Utils.keepOnlyTwoDecimals(value);
				}
			}).collect();
		} catch (Exception e) {
			e.printStackTrace();
		}

		return null;
	}

	private String[] getTwoRandomNations() {
		String nation1 = Nation.getRandomNation();
		String nation2 = Nation.getRandomNation();
		if (nation1.equals(nation2)) {
			return getTwoRandomNations();
		} else {
			return new String[] { nation1, nation2 };
		}
	}

}
