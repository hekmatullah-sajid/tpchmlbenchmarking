package de.tuberlin.dima.bdapro.spark.tpch.batch.queries;

import java.time.LocalDate;
import java.util.List;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import de.tuberlin.dima.bdapro.spark.tpch.Utils;

public class Query6 extends Query{

	public Query6(final SparkSession spark) {
		super(spark);
	}

	@Override
	public List<Row> execute() {
		return execute(Utils.getRandomInt(1993, 1997) + "-01-01", Utils.getRandomDouble(0.02, 0.09), Utils.getRandomInt(24, 25));
	}

	public List<Row> execute(final String date, final double discount, final int quantity) {
		LocalDate dateRandom = LocalDate.parse("1994-01-01");
		LocalDate interval = dateRandom.plusYears(1);

		return spark.sql("select sum(l_extendedprice*l_discount) as revenue "
				+ "from lineitem "
				+ "where l_shipdate >= '" + dateRandom.toString() + "' "
				+ "and l_shipdate < '" + interval.toString() + "' "
				+ "and l_discount between 0.05 and 0.07 "
				+ "and l_quantity < 24").collectAsList();
	}

}
