package de.tuberlin.dima.bdapro.spark.tpch.config;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * TableSourceProvider is used as a single table source provider for all 22 TPCH queries.
 * @author Hekmatullah Sajid and Seema Narasimha Swamy
 *
 */
public class TableSourceProvider {
	
	private String baseDir = PathConfig.BASE_DIR;

	public String getBaseDir(){
		return baseDir;
	}
	
	/**
	 * @param path is used in the main class to pass the path to directory where data sets are located.
	 */
	public void setBaseDir(final String path){
		baseDir = path;
	}

	/**
	 * Method used to read the data in the eight data sets by calling related method, and create the TempView in the SparkSession.
	 * @param spark, refers to the SparkSession passed from main method, the same SparkSession is used in queries for executing Spark SQL.
	 * @param sf, defines the scale factor of data sets.
	 * @return the SparkSession with TempViews created.
	 */
	public SparkSession loadData(final SparkSession spark, final String sf) {
		StructField[] lineitem = new StructField[] {
				DataTypes.createStructField("l_orderkey", DataTypes.IntegerType, false),
				DataTypes.createStructField("l_partkey", DataTypes.IntegerType, false),
				DataTypes.createStructField("l_suppkey", DataTypes.IntegerType, false),
				DataTypes.createStructField("l_linenumber", DataTypes.IntegerType, false),
				DataTypes.createStructField("l_quantity", DataTypes.DoubleType, false),
				DataTypes.createStructField("l_extendedprice", DataTypes.DoubleType, false),
				DataTypes.createStructField("l_discount", DataTypes.DoubleType, false),
				DataTypes.createStructField("l_tax", DataTypes.DoubleType, false),
				DataTypes.createStructField("l_returnflag", DataTypes.StringType, false),
				DataTypes.createStructField("l_linestatus", DataTypes.StringType, false),
				DataTypes.createStructField("l_shipdate", DataTypes.StringType, false),
				DataTypes.createStructField("l_commitdate", DataTypes.StringType, false),
				DataTypes.createStructField("l_receiptdate", DataTypes.StringType, false),
				DataTypes.createStructField("l_shipinstruct", DataTypes.StringType, false),
				DataTypes.createStructField("l_shipmode", DataTypes.StringType, false),
				DataTypes.createStructField("l_comment", DataTypes.StringType, false) };

		spark.read().option("delimiter", "|").schema(new StructType(lineitem))
		.csv(baseDir + sf + "/" + PathConfig.LINEITEM)
		.createOrReplaceTempView("lineitem");

		StructField[] supplier = new StructField[] {
				DataTypes.createStructField("s_suppkey", DataTypes.IntegerType, false),
				DataTypes.createStructField("s_name", DataTypes.StringType, false),
				DataTypes.createStructField("s_address", DataTypes.StringType, false),
				DataTypes.createStructField("s_nationkey", DataTypes.IntegerType, false),
				DataTypes.createStructField("s_phone", DataTypes.StringType, false),
				DataTypes.createStructField("s_acctbal", DataTypes.DoubleType, false),
				DataTypes.createStructField("s_comment", DataTypes.StringType, false) };

		spark.read().option("delimiter", "|").schema(new StructType(supplier))
		.csv(baseDir + sf + "/" + PathConfig.SUPPLIER)
		.createOrReplaceTempView("supplier");

		StructField[] part = new StructField[] {
				DataTypes.createStructField("p_partkey", DataTypes.IntegerType, false),
				DataTypes.createStructField("p_name", DataTypes.StringType, false),
				DataTypes.createStructField("p_mfgr", DataTypes.StringType, false),
				DataTypes.createStructField("p_brand", DataTypes.StringType, false),
				DataTypes.createStructField("p_type", DataTypes.StringType, false),
				DataTypes.createStructField("p_size", DataTypes.IntegerType, false),
				DataTypes.createStructField("p_container", DataTypes.StringType, false),
				DataTypes.createStructField("p_retailprice", DataTypes.DoubleType, false),
				DataTypes.createStructField("p_comment", DataTypes.StringType, false) };

		spark.read().option("delimiter", "|").schema(new StructType(part))
		.csv(baseDir + sf + "/" + PathConfig.PART)
		.createOrReplaceTempView("part");

		StructField[] partsupp = new StructField[] {
				DataTypes.createStructField("ps_partkey", DataTypes.IntegerType, false),
				DataTypes.createStructField("ps_suppkey", DataTypes.IntegerType, false),
				DataTypes.createStructField("ps_availqty", DataTypes.IntegerType, false),
				DataTypes.createStructField("ps_supplycost", DataTypes.DoubleType, false),
				DataTypes.createStructField("ps_comment", DataTypes.StringType, false) };

		spark.read().option("delimiter", "|").schema(new StructType(partsupp))
		.csv(baseDir + sf + "/" + PathConfig.PARTSUPP)
		.createOrReplaceTempView("partsupp");

		StructField[] customer = new StructField[] {
				DataTypes.createStructField("c_custkey", DataTypes.IntegerType, false),
				DataTypes.createStructField("c_name", DataTypes.StringType, false),
				DataTypes.createStructField("c_address", DataTypes.StringType, false),
				DataTypes.createStructField("c_nationkey", DataTypes.IntegerType, false),
				DataTypes.createStructField("c_phone", DataTypes.StringType, false),
				DataTypes.createStructField("c_acctbal", DataTypes.DoubleType, false),
				DataTypes.createStructField("c_mktsegment", DataTypes.StringType, false),
				DataTypes.createStructField("c_comment", DataTypes.StringType, false)};

		spark.read().option("delimiter", "|").schema(new StructType(customer))
		.csv(baseDir + sf + "/" + PathConfig.CUSTOMER)
		.createOrReplaceTempView("customer");

		StructField[] nation = new StructField[] {
				DataTypes.createStructField("n_nationkey", DataTypes.IntegerType, false),
				DataTypes.createStructField("n_name", DataTypes.StringType, false),
				DataTypes.createStructField("n_regionkey", DataTypes.IntegerType, false),
				DataTypes.createStructField("n_comment", DataTypes.StringType, false) };

		spark.read().option("delimiter", "|").schema(new StructType(nation))
		.csv(baseDir + sf + "/" + PathConfig.NATION)
		.createOrReplaceTempView("nation");

		StructField[] region = new StructField[] {
				DataTypes.createStructField("r_regionkey", DataTypes.IntegerType, false),
				DataTypes.createStructField("r_name", DataTypes.StringType, false),
				DataTypes.createStructField("r_comment", DataTypes.StringType, false) };

		spark.read().option("delimiter", "|").schema(new StructType(region))
		.csv(baseDir + sf + "/" + PathConfig.REGION)
		.createOrReplaceTempView("region");

		StructField[] orders = new StructField[] {
				DataTypes.createStructField("o_orderkey", DataTypes.IntegerType, false),
				DataTypes.createStructField("o_custkey", DataTypes.IntegerType, false),
				DataTypes.createStructField("o_orderstatus", DataTypes.StringType, false),
				DataTypes.createStructField("o_totalprice", DataTypes.DoubleType, false),
				DataTypes.createStructField("o_orderdate", DataTypes.StringType, false),
				DataTypes.createStructField("o_orderpriority", DataTypes.StringType, false),
				DataTypes.createStructField("o_clerk", DataTypes.StringType, false),
				DataTypes.createStructField("o_shipriority", DataTypes.IntegerType, false),
				DataTypes.createStructField("o_comment", DataTypes.StringType, false) };

		spark.read().option("delimiter", "|").schema(new StructType(orders))
		.csv(baseDir + sf + "/" + PathConfig.ORDERS)
		.createOrReplaceTempView("orders");

		return spark;
	}

}
