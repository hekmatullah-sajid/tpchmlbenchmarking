package de.tuberlin.dima.bdapro.flink.tpch.batch.queries;

import java.util.List;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;

import de.tuberlin.dima.bdapro.flink.tpch.Utils;

public class Query6 extends Query {

	public Query6(final BatchTableEnvironment env) {
		super(env);
	}

	@Override
	public List<Tuple1<Double>> execute() {
		return execute(Utils.getRandomInt(1993, 1997) + "-01-01", Utils.getRandomDouble(0.02, 0.09), Utils.getRandomInt(24, 25));
	}

	public List<Tuple1<Double>> execute(final String date, final double discount, final int quantity) {
		Table lineitem = env.scan("lineitem");

		double lowerBoundDiscount = Utils.convertToTwoDecimal(discount - 0.01);
		double upperBoundDiscount = Utils.convertToTwoDecimal(discount + 0.01); 

		Table result = lineitem.where("l_shipdate.toDate >= '" + date + "'.toDate ")
				.where("l_shipdate.toDate < ('" + date + "'.toDate + 1.year) ")
				.where("l_discount >= " + lowerBoundDiscount)
				.where("l_discount <= " + upperBoundDiscount)
				.where("l_quantity < " + quantity)
				.select("sum(l_extendedprice*l_discount) as revenue");

		try {
			return env.toDataSet(result, TypeInformation.of
					(new TypeHint<Tuple1<Double>>(){}))
					.map(new MapFunction<Tuple1<Double>, Tuple1<Double>>() {
						private static final long serialVersionUID = 1L;

						@Override
						public Tuple1<Double> map(
								final Tuple1<Double> value)
										throws Exception {
							return Utils.keepOnlyTwoDecimals(value);
						}
					}).collect();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;

	}

}
