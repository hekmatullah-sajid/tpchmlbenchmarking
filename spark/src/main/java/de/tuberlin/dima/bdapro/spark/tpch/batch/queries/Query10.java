package de.tuberlin.dima.bdapro.spark.tpch.batch.queries;

import java.util.List;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import de.tuberlin.dima.bdapro.spark.tpch.Utils;

public class Query10 extends Query {

	public Query10(final SparkSession spark) {
		super(spark);
	}

	@Override
	public List<Row> execute() {
		return execute(getRandomDate());
	}

	public List<Row> execute(final String randomDate) {
		return spark.sql("select c_custkey, c_name, "
				+ "sum(l_extendedprice * (1 - l_discount)) as revenue, "
				+ "c_acctbal, n_name, c_address, c_phone, c_comment "
				+ "from customer, orders, lineitem, nation "
				+ "where c_custkey = o_custkey "
				+ "and l_orderkey = o_orderkey "
				+ "and o_orderdate >= date '" + randomDate + "' "
				+ "and o_orderdate < date '" + randomDate + "' + interval '3' month "
				+ "and l_returnflag = 'R' "
				+ "and c_nationkey = n_nationkey "
				+ "group by c_custkey, c_name, c_acctbal, c_phone, n_name, "
				+ "c_address, c_comment order by revenue desc").collectAsList();
	}

	private String getRandomDate() {
		int year = Utils.getRandomInt(1993, 1995);
		int month = Utils.getRandomInt(1, 12);
		if (month == 1 && year == 1993) {
			month = Utils.getRandomInt(2, 12);
		}
		String monthString;
		if (month < 10) {
			monthString = "-0" + month;
		} else {
			monthString = "-" + month;
		}
		return year + monthString + "-01";
	}

}
