package de.tuberlin.dima.bdapro.spark.tpch.batch.queries;

import java.util.List;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import de.tuberlin.dima.bdapro.spark.tpch.Utils.Nation;

public class Query11 extends Query {

	private double sf;

	public Query11(final SparkSession spark, final String sf) {
		super(spark);
		this.sf = Double.parseDouble(sf);
	}

	@Override
	public List<Row> execute() {
		return execute(Nation.getRandomNation(), 0.0001/sf);
	}

	public List<Row> execute(final String randomNation, final double fraction) {
		return spark.sql(
				"SELECT ps_partkey, sum(ps_supplycost * ps_availqty) AS sorted "
						+ "FROM partsupp, supplier, nation "
						+ "WHERE ps_suppkey = s_suppkey and s_nationkey = n_nationkey "
						+ "and n_name = '" + randomNation + "' "
						+ "GROUP BY ps_partkey HAVING "
						+ "sum(ps_supplycost * ps_availqty) > ("
						+ "SELECT sum(ps_supplycost * ps_availqty) * " + fraction 
						+ " FROM partsupp, supplier, nation "
						+ "WHERE ps_suppkey = s_suppkey and s_nationkey = n_nationkey "
						+ "and n_name = '" + randomNation + "') "
						+ "ORDER BY sorted desc").collectAsList();
	}

}
