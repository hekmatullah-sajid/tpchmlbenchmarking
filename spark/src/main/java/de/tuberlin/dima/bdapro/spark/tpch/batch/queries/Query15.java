package de.tuberlin.dima.bdapro.spark.tpch.batch.queries;

import java.time.LocalDate;
import java.util.List;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import de.tuberlin.dima.bdapro.spark.tpch.Utils;


public class Query15 extends Query {
	
	public Query15(final SparkSession spark) {
		super(spark);
	}

	@Override
	public List<Row> execute() {
		return execute(LocalDate.parse(Utils.getRandomInt(1993, 1997) + "-01-01"));
	}
	
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