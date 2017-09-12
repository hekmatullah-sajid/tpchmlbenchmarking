package de.tuberlin.dima.bdapro.spark.tpch.batch.queries;

import static org.apache.spark.sql.functions.callUDF;

import java.io.Serializable;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;

import de.tuberlin.dima.bdapro.spark.tpch.config.Utils.Nation;

/**
 * Volume Shipping Query (Q7), TPC-H Benchmark Specification page 39 http://www.tpc.org/tpc_documents_current_versions/pdf/tpc-h_v2.17.2.pdf). 
 * @author Hekmatullah Sajid and Seema Narasimha Swamy
 *
 */
public class Query7 extends Query implements Serializable {
	private static final long serialVersionUID = 1L;

	public Query7(){
		super();
	}

	public Query7(final SparkSession spark) {
		super(spark);
	}

	/**
	 * Find the random values and pass it to the execute method (with parameter).
	 */
	@Override
	public List<Row> execute() {
		String[] randNations = getTwoRandomNations();
		return execute(randNations[0], randNations[1]);
	}

	/**
	 * Execute Query7 of TPC-H and returns the result.
	 * The direct SQL and the SQL functions in the query (extract(year from l_shipdate) AND l_extendedprice * (1 - l_discount)) were not supported by Spark SQL.
	 * The query is executed using Spark DataFrame to get the result.
	 * @param nation1 is randomly selected within the list of values defined in enum Nation.
	 * @param nation2 is randomly selected within the list of values defined in enum Nation, and should be different from the nation1.
	 * @return the result of the query
	 */
	public List<Row> execute(final String nation1, final String nation2) {
		spark.udf().register("volume", new UDF2<Double, Double, Double>() {
			private static final long serialVersionUID = 8504889569988140680L;

			@Override
			public Double call(final Double extPrice, final Double discount) {
				return extPrice * (1 - discount);
			}
		}, DataTypes.DoubleType);

		spark.udf().register("getYear", new UDF1<String,String>() {
			private static final long serialVersionUID = 7916629676707870056L;
			@Override
			public String call(final String date) {
				return date.substring(0, 4);
			}
		}, DataTypes.StringType);

		Dataset<Row> lineitem = spark.table("lineitem")
				.filter("l_shipdate >= date '1995-01-01'")
				.filter("l_shipdate <= date '1996-12-31'");

		Dataset<Row> supplier = spark.table("supplier");
		Dataset<Row> orders = spark.table("orders");
		Dataset<Row> customer = spark.table("customer");
		Dataset<Row> nation1Table = spark.table("nation")
				.toDF("n1_nationkey", "n1_name", "n1_regionkey", "n1_comment")
				.filter("n1_name = '" + nation1 + "'");
		Dataset<Row> nation2Table = spark.table("nation")
				.toDF("n2_nationkey", "n2_name", "n2_regionkey", "n2_comment")
				.filter("n2_name = '" + nation2 + "'");

		return supplier.join(nation1Table).where("s_nationkey = n1_nationkey").join(lineitem)
				.where("s_suppkey = l_suppkey").join(orders).where("o_orderkey = l_orderkey")
				.join(customer).where("c_custkey = o_custkey").join(nation2Table)
				.where("c_nationkey = n2_nationkey")
				.select(nation1Table.col("n1_name").as("supp_nation"), 
						nation2Table.col("n2_name").as("cust_nation"), 
						callUDF("getYear", lineitem.col("l_shipdate")).as("l_year"),
						callUDF("volume", lineitem.col("l_extendedprice"), lineitem.col("l_discount"))
						.as("volume"))
				.orderBy("supp_nation", "cust_nation", "l_year")
				.groupBy("supp_nation", "cust_nation", "l_year").sum("volume")
				.collectAsList();
	}

	/**
	 * Get the substitution parameters random nation1 and nation2, where nation2 is not equal to nation1.
	 * @return array of randomly selected nations
	 */
	private String[] getTwoRandomNations() {
		String nation1 = Nation.getRandomNation();
		String nation2 = Nation.getRandomNation();
		if (nation1.equals(nation2)) {
			return getTwoRandomNations();
		} else {
			return new String[] { nation1, nation2 };
		}
	}


}
