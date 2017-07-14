package de.tuberlin.dima.bdapro.spark.tpch.batch.queries;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import de.tuberlin.dima.bdapro.spark.tpch.Utils;

public class Query22 extends Query {

	public Query22(SparkSession spark) {
		super(spark);
	}

	@Override
	public List<Row> execute() {
        return execute(getRandomCountryCode());
	}
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
