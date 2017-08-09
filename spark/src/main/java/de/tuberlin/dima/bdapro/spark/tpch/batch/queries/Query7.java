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

import de.tuberlin.dima.bdapro.spark.tpch.Utils.Nation;

public class Query7 extends Query implements Serializable {
	private static final long serialVersionUID = 1L;

	public Query7(){
		super();
	}

	public Query7(final SparkSession spark) {
		super(spark);
	}

	@Override
	public List<Row> execute() {
		String[] randNations = getTwoRandomNations();
		return execute(randNations[0], randNations[1]);
	}

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

//				return spark.sql("select supp_nation, cust_nation, year, sum(volume) as revenue "
//						+ "from ( "
//						+ "select n1.n_name as supp_nation, "
//						+ "n2.n_name as cust_nation, "
//						+ "extract(year from l_shipdate) as year "
//						+ "l_extendedprice * (1 - l_discount) as volume "
//						+ "from supplier, lineitem, orders, customer, nation n1, nation n2 "
//						+ "where s_suppkey = l_suppkey "
//						+ "and o_orderkey = l_orderkey "
//						+ "and c_custkey = o_custkey "
//						+ "and s_nationkey = n1.n_nationkey "
//						+ "and c_nationkey = n2.n_nationkey "
//						+ "and ( "
//						+ "(n1.n_name = '" + nation1 + "' and n2.n_name = '" + nation2 + "') "
//						+ "or (n1.n_name = '" + nation2 + "' and n2.n_name = '" + nation1 + "') ) "
//						+ "and l_shipdate between '1995-01-01' and '1996-12-31' ) shipping"
//						+ "group by supp_nation, cust_nation, year "
//						+ "order by supp_nation, cust_nation, year").collectAsList();
	}

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
