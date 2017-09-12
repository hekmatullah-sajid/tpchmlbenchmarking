package de.tuberlin.dima.bdapro.spark.tpch.batch.queries;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import de.tuberlin.dima.bdapro.spark.tpch.config.Utils;

/**
 * Global Sales Opportunity Query (Q22), TPC-H Benchmark Specification page 66 http://www.tpc.org/tpc_documents_current_versions/pdf/tpc-h_v2.17.2.pdf). 
 * @author Hekmatullah Sajid
 *
 */
public class Query22 extends Query {

	public Query22(SparkSession spark) {
		super(spark);
	}

	/**
	 * Find the random values and pass it to the execute method (with parameter).
	 */
	@Override
	public List<Row> execute() {
        return execute(getRandomCountryCode());
	}
	
	 /**
	 * Executes Query22 of TPC-H and returns the result.
	 * The substring method syntax as written in TPC-H query 22 (substring (c_phone from 1 for 2) was not supported by Spark SQL, 
	 * the change in syntax solved the error, which Spark SQL was not able to create logical plan for the query, the query executed without any error.
     * @param countryCodes are randomly selected without repetition from the possible values for Country code.
     * @return result of the query
     */
    public List<Row> execute(List<Integer> countryCodes) {
    	String querySQL = "SELECT cntrycode, count(*)  numcust, sum(c_acctbal)  totacctbal "
                + "FROM ( SELECT substring(c_phone, 1, 2)  cntrycode, c_acctbal "
                + "FROM customer "
                + "WHERE substring(c_phone, 1, 2) in "
                + "('" + countryCodes.get(0) + "', ' " + countryCodes.get(1) + "', '" + countryCodes.get(2) + "', '"
                + countryCodes.get(3) + "', '" + countryCodes.get(4) + "', '" + countryCodes.get(5) + "', '"
                + countryCodes.get(6)  + "' ) "
                + "AND c_acctbal > ( SELECT avg(c_acctbal) FROM customer "
                + "WHERE c_acctbal > 0.00 "
                + "AND substring (c_phone, 1, 2) in "
                + "(' " + countryCodes.get(0) + "', ' " + countryCodes.get(1) + "', '" + countryCodes.get(2) + "', ' "
                + countryCodes.get(3) + "', '" + countryCodes.get(4) + "', '" + countryCodes.get(5) + "', '"
                + countryCodes.get(6)  + "') ) "
                + "AND NOT EXISTS ( "
                + "SELECT * FROM orders WHERE o_custkey = c_custkey "
                + ") )  custsale "
                + "GROUP BY cntrycode ORDER BY cntrycode";
    	
		return spark.sql(querySQL).collectAsList();
    }
    
    /**
     * Query 22 of TPC-H Benchmark needs a substitution parameter that must be generated and used to build the executable query text.
     * The substitution parameter is Country codes that are randomly selected without repetition from the possible values for Country code.
     * @return a list of unique and randomly selected country codes
     */
    private List<Integer>getRandomCountryCode() {
        Set<Integer> countrycodes = new LinkedHashSet<>();
        while (countrycodes.size() < 7)
        {
            Integer next = Utils.getRandomInt(0,24) + 10;
            // As we're adding to a set, this will automatically do a containment check
            countrycodes.add(next);
        }
        return new ArrayList<>(countrycodes);
    }

}
