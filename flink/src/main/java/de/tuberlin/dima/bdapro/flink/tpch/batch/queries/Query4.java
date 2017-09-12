package de.tuberlin.dima.bdapro.flink.tpch.batch.queries;

import java.time.LocalDate;
import java.util.List;
import java.util.Random;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;

/**
 * Order Priority Checking Query (Q4), TPC-H Benchmark Specification page 35
 * http://www.tpc.org/tpc_documents_current_versions/pdf/tpc-h_v2.17.2.pdf).
 * 
 * @author Seema Narasimha Swamy
 *
 */
public class Query4 extends Query {
	public Query4(final BatchTableEnvironment env) {
		super(env);
	}

	/**
	 * Find the random values and pass it to the execute method (with
	 * parameter).
	 */
	@Override
	public List<Tuple2<String, Long>> execute() {
		return execute(getRandomDate());
	}

	/**
	 * Executes Query4 of TPC-H and returns the result.
	 * 
	 * @param date
	 *            is the first day of a randomly selected month between the
	 *            first month of 1993 and the 10th month of 1997.
	 * @return result of the query
	 */
	public List<Tuple2<String, Long>> execute(LocalDate rndDate) {

		String SQLQuery = "SELECT o_orderpriority,count( * ) as order_count " + "FROM orders "
				+ "WHERE o_orderdate >= '" + rndDate.toString() + "' and " + "o_orderdate < '"
				+ rndDate.plusMonths(3).toString() + "' " + "and exists ( " + "SELECT * FROM lineitem "
				+ "WHERE l_orderkey = o_orderkey and  l_commitdate < l_receiptdate ) "
				+ "GROUP BY o_orderpriority ORDER BY o_orderpriority";

		Table res = env.sql(SQLQuery);

		/*
		 * Drop more than two decimal values in double values. And return the
		 * result.
		 */
		try {
			return env.toDataSet(res, TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {
			})).collect();
		} catch (Exception e) {
			e.printStackTrace();
		}

		return null;
	}

	/**
	 * Get the substitution parameter Date, randomly selected month between the
	 * first month of 1993 and the 10th month of 1997.
	 * 
	 * @return array of randomly selected nations
	 */
	private LocalDate getRandomDate() {
		Random rand = new Random();
		int year = rand.nextInt((5) + 1993);
		int month;
		if (year < 1997) {
			month = rand.nextInt(12) + 1;// ) is inclusive in nextInt, so add 1
		} else {
			month = rand.nextInt(10) + 1;
		}
		return LocalDate.of(year, month, 1);
	}
}
