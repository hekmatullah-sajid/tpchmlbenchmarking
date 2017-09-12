package de.tuberlin.dima.bdapro.spark.tpch.batch.queries;

import java.time.LocalDate;
import java.util.List;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import de.tuberlin.dima.bdapro.spark.tpch.config.Utils;
/**
 * Pricing Summary Report Query (Q1), TPC-H Benchmark Specification page 29 http://www.tpc.org/tpc_documents_current_versions/pdf/tpc-h_v2.17.2.pdf). 
 * @author Hekmatullah Sajid andSeema Narasimha Swamy
 *
 */
public class Query1 extends Query{

	public Query1(final SparkSession spark) {
		super(spark);
	}

	/**
	 * The parameter delta should be randomly selected within [60. 120].
	 */
	@Override
	public List<Row> execute() {
		return execute(Utils.getRandomInt(60, 120));
	}
	/**
	 * Executes Query1 of TPC-H and returns the result.
	 * @param delta, is substitution parameter that must be generated and used to build the executable query text.
	 * @return the result of the query.
	 */
	public List<Row> execute(final int delta) {
		String dateThreshold = LocalDate.parse("1998-12-01").minusDays(delta).toString();

		return spark.sql("select l_returnflag, "
				+ "l_linestatus, "
				+ "sum(l_quantity) as sum_qty, "
				+ "sum(l_extendedprice) as sum_base_price, "
				+ "sum(l_extendedprice*(1-l_discount)) as sum_disc_price, "
				+ "sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge, "
				+ "avg(l_quantity) as avg_qty, "
				+ "avg(l_extendedprice) as avg_price, "
				+ "avg(l_discount) as avg_disc, "
				+ "count(*) as count_order "
				+ "from lineitem "
				+ "where l_shipdate <= '" + dateThreshold + "' "
				+ "group by l_returnflag, l_linestatus "
				+ "order by l_returnflag, l_linestatus").collectAsList();

	}

}
