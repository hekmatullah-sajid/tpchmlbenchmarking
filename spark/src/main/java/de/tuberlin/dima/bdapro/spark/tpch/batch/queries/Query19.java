package de.tuberlin.dima.bdapro.spark.tpch.batch.queries;

import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import de.tuberlin.dima.bdapro.spark.tpch.Utils;


public class Query19 extends Query {
	
	public Query19(SparkSession spark) {
		super(spark);
	}
	
	@Override
	public List<Row> execute() {
		return execute(Utils.getRandomBrand(), Utils.getRandomBrand(), Utils.getRandomBrand(), Utils.getRandomInt(1, 10), Utils.getRandomInt(10, 20), Utils.getRandomInt(20, 30));
	}
	public List<Row> execute(String brand1, String brand2, String brand3, int qty1, int qty2, int qty3) {
		
//		String querySQL1 = "SELECT sum(l_extendedprice * (1 - l_discount) ) as revenue from lineitem, part where (p_partkey = l_partkey "
//				+ "and p_brand = '" + brand1 + "' and p_container in ( 'SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') "
//				+ "and l_quantity >= " + qty1 + " and l_quantity <= " + (qty1 + 10) + " and p_size between 1 and 5 "
//				+ "and l_shipmode in ('AIR', 'AIR REG') and l_shipinstruct = 'DELIVER IN PERSON' ) ";
//		
//		String querySQL2 = "SELECT sum(l_extendedprice * (1 - l_discount) ) as revenue from lineitem, part "
//				+ "where  (p_partkey = l_partkey and p_brand = '" + brand2 + "' and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK') "
//				+ "and l_quantity >= " + qty2 + " and l_quantity <= " + (qty2 + 10) + " "
//				+ "and p_size between 1 and 10 and l_shipmode in ('AIR', 'AIR REG') and l_shipinstruct = 'DELIVER IN PERSON')";
//		String querySQL3 = "SELECT sum(l_extendedprice * (1 - l_discount) ) as revenue from lineitem, part "
//				+ "where  (p_partkey = l_partkey and p_brand = '" + brand3 + "' and p_container in ( 'LG CASE', 'LG BOX', 'LG PACK', 'LG PKG') "
//				+ "and l_quantity >= " + qty3 + " and l_quantity <= " + (qty3 + 10) + " and p_size between 1 and 15 "
//				+ "and l_shipmode in ('AIR', 'AIR REG') and l_shipinstruct = 'DELIVER IN PERSON')";
		
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
		
//		Dataset<Row> pratilaRes1 = spark.sql(querySQL1);
//		Dataset<Row> pratilaRes2 = spark.sql(querySQL2);
//		Dataset<Row> pratilaRes3 = spark.sql(querySQL3);
		
//		Dataset<Row> res = pratilaRes1.union(pratilaRes2).union(pratilaRes3).select("SUM(revenue)").collectAsList()
		//return pratilaRes1.union(pratilaRes2).union(pratilaRes3).agg(org.apache.spark.sql.functions.sum("revenue")).collectAsList();
		return spark.sql(querySQL).collectAsList();

	}

}
