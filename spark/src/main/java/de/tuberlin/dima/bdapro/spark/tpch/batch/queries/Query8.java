package de.tuberlin.dima.bdapro.spark.tpch.batch.queries;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.sum;

import java.io.Serializable;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;

import de.tuberlin.dima.bdapro.spark.tpch.config.Utils;
import de.tuberlin.dima.bdapro.spark.tpch.config.Utils.Nation;

/**
 * National Market Share Query (Q8), TPC-H Benchmark Specification page 41 http://www.tpc.org/tpc_documents_current_versions/pdf/tpc-h_v2.17.2.pdf). 
 * @author Hekmatullah Sajid and Seema Narasimha Swamy
 *
 */
public class Query8 extends Query implements Serializable{
	private static final long serialVersionUID = 1L;

	public Query8() {
		super();
	}

	public Query8(final SparkSession spark) {
		super(spark);
	}

	/**
	 * Find the random values and pass it to the execute method (with parameter).
	 */
	@Override
	public List<Row> execute() {
		Nation nation = Nation.getRandomNationAndRegion();
		return execute(nation.getName(), nation.getRegion(), Utils.getRandomType());
	}

	/**
	 * Executes Query8 of TPC-H and returns the result.
	 * The CASE SQL control structure was not supported by Spark SQL (for more interpretation please check the query 8 SQL on TPC-H Benchmark Specification document page 41).
	 * Spark could not generate logical plan for the given SQL.
	 * For performing the unsupported SQL, UDFs are defined to get the query result using the combination of Spark SQL and DataFrame.
	 * @param nation  is randomly selected within the list of Nation
	 * @param region is randomly selected within the list of Regions
	 * @param randomType is randomly selected from the lists TYPE_SYL1, TYPE_SYL2 and TYPE_SYL3.
	 * @return result of the query
	 */
	public List<Row> execute(final String nation, final String region, final String randomType) {
		spark.udf().register("volumeFilter", new UDF2<Double, String, Double>() {
			private static final long serialVersionUID = 8504889569988140680L;

			@Override
			public Double call(final Double volume, final String nation2) {
				if(nation2.equals(nation)){
					return volume;
				}
				return 0.0;
			}
		}, DataTypes.DoubleType);

		spark.udf().register("division", new UDF2<Double, Double, Double>() {
			private static final long serialVersionUID = 8504889569988140680L;

			@Override
			public Double call(final Double volume, final Double case2) {
				return case2 / volume;
			}
		}, DataTypes.DoubleType);

		Dataset<Row> innerRes = spark.sql("select year(o_orderdate) as o_year, "
				+ "l_extendedprice * (1-l_discount) as volume, "
				+ "n2.n_name as nation "
				+ "from part,supplier,lineitem,orders,customer,nation n1,nation n2,region "
				+ "where p_partkey = l_partkey "
				+ "and s_suppkey = l_suppkey "
				+ "and l_orderkey = o_orderkey "
				+ "and o_custkey = c_custkey "
				+ "and c_nationkey = n1.n_nationkey "
				+ "and n1.n_regionkey = r_regionkey "
				+ "and r_name = '" + region + "' "
				+ "and s_nationkey = n2.n_nationkey "
				+ "and o_orderdate between date '1995-01-01' and date '1996-12-31' "
				+ "and p_type = '" + randomType + "' ");

		return innerRes.select(innerRes.col("o_year"), 
				callUDF("volumeFilter", innerRes.col("volume"), innerRes.col("nation")).as("case"), 
				innerRes.col("volume"))
				.groupBy("o_year")
				.agg(sum("case"), sum("volume"))
				.orderBy("o_year")
				.select(innerRes.col("o_year"),
						callUDF("division", col("sum(volume)"), col("sum(case)")).as("mkt_share"))
				.collectAsList();

	}

}
