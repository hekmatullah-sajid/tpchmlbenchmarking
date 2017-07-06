package de.tuberlin.dima.bdapro.flink.tpch.streaming.queries;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple10;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.api.java.Tumble;

import de.tuberlin.dima.bdapro.flink.tpch.Utils;

public class Query1 extends Query {

	public Query1(final StreamTableEnvironment env, final String sf) {
		super(env, sf);
	}

	@Override
	public void execute() {
		execute(Utils.getRandomInt(60, 120));
	}

	public void execute(final int delta) {
		Table lineitem = env.ingest("lineitem");

		Table result = lineitem
				.window(Tumble.over("100000.rows").on("rowtime").as("w"))				
				.groupBy("w, returnflag, linestatus")
				.select("returnflag, linestatus, sum(quantity) as sum_qty, "
						+ "sum(extendedprice) as sum_base_price, "
						+ "sum(extendedprice*(1-discount)) as sum_disc_price, "
						+ "sum(extendedprice*(1-discount)*(1+tax)) as sum_charge, "
						+ "avg(quantity) as avg_qty, "
						+ "avg(extendedprice) as avg_price, "
						+ "avg(discount) as avg_disc, "
						+ "count(linestatus) as count_order")
				.where("shipdate.toDate <= ('1998-12-01'.toDate - " + delta + ".days)")
				.orderBy("returnflag, linestatus");

		try {
			env.toDataStream(result, TypeInformation.of
					(new TypeHint<Tuple10<String, String, Double, Double, Double, Double, Double, Double, Double, Long>>(){}))
			.map(new MapFunction<Tuple10<String,String,Double,Double,Double,Double,Double,Double,Double,Long>, Tuple10<String, String, Double, Double, Double, Double, Double, Double, Double, Long>>() {
				private static final long serialVersionUID = 1L;

				@Override
				public Tuple10<String, String, Double, Double, Double, Double, Double, Double, Double, Long> map(
						final Tuple10<String, String, Double, Double, Double, Double, Double, Double, Double, Long> value)
								throws Exception {
					return Utils.keepOnlyTwoDecimals(value);
				}
			});
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
