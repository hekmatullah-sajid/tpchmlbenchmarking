package de.tuberlin.dima.bdapro.spark.tpch.batch.queries;

import java.time.LocalDate;
import java.util.List;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import de.tuberlin.dima.bdapro.spark.tpch.config.Utils;

/**
 * Shipping Modes and Order Priority Query (Q12), TPC-H Benchmark Specification page 49 http://www.tpc.org/tpc_documents_current_versions/pdf/tpc-h_v2.17.2.pdf). 
 * @author Hekmatullah Sajid
 *
 */
public class Query12 extends Query {

	public Query12(SparkSession spark) {
		super(spark);
	}

	/**
	 * Find the random values and pass it to the execute method (with parameter).
	 */
	@Override
	public List<Row> execute() {
		String shipMode1 = Utils.getRandomShipmode();
		String shipMode2 = "";
		do{
			shipMode2 = Utils.getRandomShipmode();
		} while (shipMode1.equals(shipMode2));
		return execute(shipMode1, shipMode2, LocalDate.parse(Utils.getRandomInt(1993, 1997) + "-01-01"));
	}
	
	/**
	 * Executes Query12 of TPC-H and returns the result.
	 * @param rndShipmode1 is randomly selected within the SHIPMODES list of values.
	 * @param rndShipmode2 is randomly selected within the SHIPMODES list of values, and must be different from the value selected for rndShipmode1.
	 * @param rndDate is the first of January of a randomly selected year within [1993 .. 1997].
	 * @return result of the query
	 */
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
