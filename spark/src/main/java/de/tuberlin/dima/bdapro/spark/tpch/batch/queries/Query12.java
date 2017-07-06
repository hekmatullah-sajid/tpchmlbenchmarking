package de.tuberlin.dima.bdapro.spark.tpch.batch.queries;

import java.time.LocalDate;
import java.util.List;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import de.tuberlin.dima.bdapro.spark.tpch.Utils;


public class Query12 extends Query {

	public Query12(SparkSession spark) {
		super(spark);
	}

	@Override
	public List<Row> execute() {
		return execute(Utils.getRandomShipmode(), Utils.getRandomShipmode(), LocalDate.parse(Utils.getRandomInt(1993, 1997) + "-01-01"));
	}
	
	public List<Row> execute(String rndShipmode1, String rndShipmode2, LocalDate rndDate) {
		
		String querySQL = "SELECT l_shipmode, sum(case when o_orderpriority ='1-URGENT' or o_orderpriority ='2-HIGH' then 1 else 0 end) as high_line_count, "
				+ "sum(case when o_orderpriority <> '1-URGENT' and o_orderpriority <> '2-HIGH' then 1 else 0 end) as low_line_count "
				+ "from orders, lineitem where o_orderkey = l_orderkey and l_shipmode in ('" + rndShipmode1 + "', '" + rndShipmode2 + "') "
				+ "and l_commitdate < l_receiptdate and l_shipdate < l_commitdate and "
				+ "l_receiptdate >= '" + rndDate.toString() + "' and l_receiptdate < '" + rndDate.plusYears(1).toString() + "' "
				+ "group by l_shipmode order by l_shipmode"; 
		
		return spark.sql(querySQL).collectAsList();
	}

}
