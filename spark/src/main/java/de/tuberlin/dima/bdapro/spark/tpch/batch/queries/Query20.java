package de.tuberlin.dima.bdapro.spark.tpch.batch.queries;

import java.time.LocalDate;
import java.util.List;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import de.tuberlin.dima.bdapro.spark.tpch.Utils;
import de.tuberlin.dima.bdapro.spark.tpch.Utils.Nation;

public class Query20 extends Query {
	
	public Query20(SparkSession spark) {
		super(spark);
	}

	@Override
	public List<Row> execute() {
		return execute(Utils.getRandomColor(), LocalDate.parse(Utils.getRandomInt(1993, 1997) + "-01-01"), Nation.getRandomNation());
	}
	
	public List<Row> execute(String rndColor, LocalDate rndDate, String rndNation) {
		String querySQL = "SELECT s_name, s_address FROM supplier, nation WHERE s_suppkey in ("
				+ "SELECT ps_suppkey FROM partsupp WHERE ps_partkey IN (SELECT p_partkey FROM part WHERE p_name LIKE '" + rndColor + "%') "
				+ "and ps_availqty > ( SELECT 0.5 * SUM(l_quantity) FROM lineitem WHERE l_partkey = ps_partkey and l_suppkey = ps_suppkey "
				+ "and l_shipdate >= '" + rndDate.toString() + "' and l_shipdate < '" + rndDate.plusYears(1).toString() + "' ) ) and s_nationkey = n_nationkey "
				+ "and n_name = '" + rndNation +"' ORDER BY s_name";
		
		return spark.sql(querySQL).collectAsList();
	}

}
