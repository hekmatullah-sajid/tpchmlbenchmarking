package de.tuberlin.dima.bdapro.spark.tpch.batch.queries;

import java.time.LocalDate;
import java.util.List;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import de.tuberlin.dima.bdapro.spark.tpch.Utils;
import de.tuberlin.dima.bdapro.spark.tpch.Utils.Nation;

public class Query5 extends Query {
	
	public Query5(final SparkSession spark) {
		super(spark);
	}

	@Override
	public List<Row> execute() {
		return execute(Nation.getRandomRegion(), LocalDate.parse(Utils.getRandomInt(1993, 1997) + "-01-01"));
	}
	
	public List<Row> execute(String rndRegion, LocalDate rndDate) {
		
		String SQLQuery = "SELECT n_name, sum(l_extendedprice * (1 - l_discount)) as revenue "
				+ "FROM customer, orders, lineitem, supplier, nation, region "
				+ "WHERE c_custkey = o_custkey and l_orderkey = o_orderkey "
				+ "and l_suppkey = s_suppkey and c_nationkey = s_nationkey and s_nationkey = n_nationkey and n_regionkey = r_regionkey "
				+ "and r_name = '" + rndRegion +"' and "
				+ "o_orderdate >= '" + rndDate.toString() +"' and "
				+ "o_orderdate < '" + rndDate.plusYears(1).toString() + "' " 
				+ "GROUP BY n_name ORDER BY revenue desc";
		
		return spark.sql(SQLQuery).collectAsList();
	}

}
