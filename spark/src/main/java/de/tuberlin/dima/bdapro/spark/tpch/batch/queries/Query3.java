package de.tuberlin.dima.bdapro.spark.tpch.batch.queries;

import java.time.LocalDate;
import java.util.List;
import java.util.Random;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import de.tuberlin.dima.bdapro.spark.tpch.config.Utils;

/**
 * Shipping Priority Query (Q3), TPC-H Benchmark Specification page 33 http://www.tpc.org/tpc_documents_current_versions/pdf/tpc-h_v2.17.2.pdf). 
 * @author Seema Narasimha Swamy
 *
 */
public class Query3 extends Query{

    public Query3(final SparkSession spark) {
        super(spark);
    }

    /**
	 * Find the random values and pass it to the execute method (with parameter).
     */
    @Override
    public List<Row> execute() {
        String segment =  Utils.getRandomSegment();
        return execute(segment, getRandomDate());
    }
    
    /**
	 * Executes Query3 of TPC-H and returns the result.
     * @param segment is randomly selected within the SEGMENTS list of values
     * @param date is a randomly selected day within [1995-03-01 .. 1995-03-31].
	 * @return result of the query
     */
    public List<Row> execute(String segment, LocalDate date) {

        return spark.sql("select l_orderkey, sum(l_extendedprice * (1-l_discount)) as revenue, o_orderdate, o_shipriority "
                + "FROM customer, orders, lineitem "
                + "WHERE c_mktsegment = '" + segment +"' and "
                + "c_custkey = o_custkey and  l_orderkey = o_orderkey and "
                + "o_orderdate < '" + date.toString() +"' and "
                + "l_shipdate > '" + date.toString() +"' "
                + "GROUP BY l_orderkey, o_orderdate, o_shipriority ORDER BY revenue desc, o_orderdate").collectAsList();

    }
    
    /**
	 * Get the substitution parameter Date, randomly selected day within [1995-03-01 .. 1995-03-31].
	 * @return array of randomly selected nations
	 */
    private LocalDate getRandomDate()
    {
        Random rand = new Random();
        return LocalDate.of(1995, 3, rand.nextInt((31 - 1) + 1) + 1);
    }
}

