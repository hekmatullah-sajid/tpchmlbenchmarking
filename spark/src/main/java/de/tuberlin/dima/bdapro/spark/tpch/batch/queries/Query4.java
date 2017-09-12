package de.tuberlin.dima.bdapro.spark.tpch.batch.queries;

import java.time.LocalDate;
import java.util.List;
import java.util.Random;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Order Priority Checking Query (Q4), TPC-H Benchmark Specification page 35 http://www.tpc.org/tpc_documents_current_versions/pdf/tpc-h_v2.17.2.pdf). 
 * @author Seema Narasimha Swamy
 *
 */
public class Query4 extends Query {
    public Query4(final SparkSession spark) {
        super(spark);
    }

    /**
	 * Find the random values and pass it to the execute method (with parameter).
     */
    @Override
    public List<Row> execute() {
        return execute(getRandomDate());
    }

    /**
	 * Executes Query4 of TPC-H and returns the result.
     * @param date is the first day of a randomly selected month between the first month of 1993 and the 10th month of 1997.
	 * @return result of the query
     */
    public List<Row> execute(LocalDate date) {

        return spark.sql("SELECT o_orderpriority,count( * ) as order_count "
                + "FROM orders "
                + "WHERE o_orderdate >= '" + date.toString() + "' and "
                + "o_orderdate < '" + date.plusMonths(3).toString() + "' "
                + "and exists ( "
                + "SELECT * FROM lineitem "
                + "WHERE l_orderkey = o_orderkey and  l_commitdate < l_receiptdate ) "
                + "GROUP BY o_orderpriority ORDER BY o_orderpriority").collectAsList();

    }
    
    private LocalDate getRandomDate()
    {
        Random rand = new Random();
        int year = rand.nextInt((5) + 1993);
        int month;
        if(year < 1997) {
            month = rand.nextInt(12) + 1;//) is inclusive in nextInt, so add 1
        } else {
            month = rand.nextInt(10) + 1;
        }
        return LocalDate.of(year, month, 1);
    }
}
