package de.tuberlin.dima.bdapro.spark.tpch.batch.queries;

import java.time.LocalDate;
import java.util.List;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import de.tuberlin.dima.bdapro.spark.tpch.config.Utils;

/**
 * Top Supplier Query (Q15), TPC-H Benchmark Specification page 53 http://www.tpc.org/tpc_documents_current_versions/pdf/tpc-h_v2.17.2.pdf). 
 * @author Hekmatullah Sajid
 *
 */
public class Query15 extends Query {
	
	public Query15(final SparkSession spark) {
		super(spark);
	}

	/**
	 * Finds the random values and passes it to the execute method (with parameter).
	 */
	@Override
	public List<Row> execute() {
		return execute(LocalDate.parse(Utils.getRandomInt(1993, 1997) + "-01-01"));
	}

	/**
	 * Query15 first creates a view and then performs the query SQL on the created view and supplier table.
	 * The view is not supported, first Spark DataFrame is created and registered in SparkSession.
	 * Finally the query SQL is executed on the supplier table and the SparkSession created for view to get the query result.
	 * @param rndDate is the first day of a randomly selected month between the first month of 1993 and the 10th month of 1997.
	 * @return result of the query
	 */
	public List<Row> execute(final LocalDate rndDate) {
		
		String viewSQL = "SELECT l_suppkey as supplier_no, SUM(l_extendedprice * (1 - l_discount)) as total_revenue "
				+ "FROM lineitem WHERE l_shipdate >= '" + rndDate.toString() +"' "
				+ "and l_shipdate < '" + rndDate.plusMonths(3).toString() + "' GROUP BY l_suppkey";
		
		List<Row> viewRevenue = spark.sql(viewSQL).collectAsList();
		StructField[] viewStrct = new StructField[] {
				DataTypes.createStructField("supplier_no", DataTypes.IntegerType, false),
				DataTypes.createStructField("total_revenue", DataTypes.DoubleType, false)
		};
		
		spark.createDataFrame(viewRevenue, new StructType(viewStrct)).createOrReplaceTempView("viewrevenue");
		
		String resSQL = "SELECT s_suppkey, s_name, s_address, s_phone, total_revenue FROM supplier, viewrevenue "
				+ "WHERE s_suppkey = supplier_no and total_revenue = ( "
				+ "SELECT max(total_revenue) from viewrevenue ) ORDER BY s_suppkey";
		
		return spark.sql(resSQL).collectAsList();
	}

}