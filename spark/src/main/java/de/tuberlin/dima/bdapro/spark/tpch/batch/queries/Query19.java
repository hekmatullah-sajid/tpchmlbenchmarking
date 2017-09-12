package de.tuberlin.dima.bdapro.spark.tpch.batch.queries;

import java.util.List;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import de.tuberlin.dima.bdapro.spark.tpch.config.Utils;

/**
 * Discounted Revenue Query (Q19), TPC-H Benchmark Specification page 60 http://www.tpc.org/tpc_documents_current_versions/pdf/tpc-h_v2.17.2.pdf). 
 * @author Hekmatullah Sajid
 *
 */
public class Query19 extends Query {
	
	public Query19(SparkSession spark) {
		super(spark);
	}
	/**
	 * Finds the random values and passes it to the execute method (with parameter).
	 */
	@Override
	public List<Row> execute() {
		return execute(Utils.getRandomBrand(), Utils.getRandomBrand(), Utils.getRandomBrand(), Utils.getRandomInt(1, 10), Utils.getRandomInt(10, 20), Utils.getRandomInt(20, 30));
	}
	
	/**
	 * Executes Query19 of TPC-H and returns the result.
	 * @param brand1 'Brand#MN' where each MN is a two character string representing two numbers randomly and independently selected within [1 .. 5]
	 * @param brand2 'Brand#MN' where each MN is a two character string representing two numbers randomly and independently selected within [1 .. 5]
	 * @param brand3 'Brand#MN' where each MN is a two character string representing two numbers randomly and independently selected within [1 .. 5]
	 * @param qty1 is randomly selected within [1..10].
	 * @param qty2 is randomly selected within [10..20].
	 * @param qty3 is randomly selected within [20..30].
	 * @return result of the query
	 */
	public List<Row> execute(String brand1, String brand2, String brand3, int qty1, int qty2, int qty3) {
		
		String querySQL = "SELECT sum(l_extendedprice * (1 - l_discount) ) as revenue from lineitem, part where "
				+ "(p_partkey = l_partkey and p_brand = '" + brand1 + "' and p_container in ( 'SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') "
				+ "and l_quantity >= " + qty1 + " and l_quantity <= " + (qty1 + 10) + " and p_size between 1 and 5 "
				+ "and l_shipmode in ('AIR', 'AIR REG') and l_shipinstruct = 'DELIVER IN PERSON' ) "
				+ "or "
				+ "(p_partkey = l_partkey and p_brand = '" + brand2 + "' and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK') "
				+ "and l_quantity >= " + qty2 + " and l_quantity <= " + (qty2 + 10) + " "
				+ "and p_size between 1 and 10 and l_shipmode in ('AIR', 'AIR REG') and l_shipinstruct = 'DELIVER IN PERSON') "
				+ "or "
				+ "(p_partkey = l_partkey and p_brand = '" + brand3 + "' and p_container in ( 'LG CASE', 'LG BOX', 'LG PACK', 'LG PKG') "
				+ "and l_quantity >= " + qty3 + " and l_quantity <= " + (qty3 + 10) + " and p_size between 1 and 15 "
				+ "and l_shipmode in ('AIR', 'AIR REG') and l_shipinstruct = 'DELIVER IN PERSON')";

		return spark.sql(querySQL).collectAsList();

	}

}
