package de.tuberlin.dima.bdapro.spark.tpch.batch.queries;

import java.util.List;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import de.tuberlin.dima.bdapro.spark.tpch.Utils;

public class Query18 extends Query {
	
	public Query18(final SparkSession spark) {
		super(spark);
	}
	
	@Override
	public List<Row> execute() {
		// TODO Auto-generated method stub
		return execute(Utils.getRandomInt(312, 315));
	}
	
	public List<Row> execute(int rndQty) {
		String querySQL = "SELECT c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, sum(l_quantity) from customer, orders, lineitem "
				+ "WHERE o_orderkey in (SELECT l_orderkey from lineitem group by l_orderkey having sum(l_quantity) > " + rndQty +") "
				+ "and c_custkey = o_custkey and o_orderkey = l_orderkey "
				+ "GROUP BY c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice "
				+ "ORDER BY o_totalprice desc, o_orderdate limit 100";
		
		return spark.sql(querySQL).collectAsList();
		
	}

}
