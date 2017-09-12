package de.tuberlin.dima.bdapro.spark.tpch.batch.queries;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import de.tuberlin.dima.bdapro.spark.tpch.config.Utils;

/**
 * Parts/Supplier Relationship Query (Q16), TPC-H Benchmark Specification page 55 http://www.tpc.org/tpc_documents_current_versions/pdf/tpc-h_v2.17.2.pdf). 
 * @author Seema Narasimha Swamy
 *
 */
public class Query16 extends Query {
    public Query16(SparkSession spark) {
        super(spark);
    }

    /**
	 * Find the random values and pass it to the execute method (with parameter).
     */
    @Override
    public List<Row> execute() {
        return execute(Utils.getRandomBrand(),Utils.getRandomType(), getRandomSize());
    }
    
    /**
     * Executes Query16 of TPC-H and returns the result.
     * The NOT EXISTS operator in this query was not supported by Spark, it could not generate a logical plan for the QUERY SQL with NOT EXISTS,
     * the operator NOT EXISTS is replaced by NOT IN and the inner query is changed accordingly.
     * @param brand Brand#MN where M and N are two single character strings representing two numbers randomly and independently selected within [1 .. 5].
     * @param type is made of the first 2 syllables of a string randomly selected from the lists TYPE_SYL1, TYPE_SYL2 and TYPE_SYL3.
     * @param sizeArray is randomly selected as a set of eight different values within [1 .. 50].
     * @return result of the query
     */
    public List<Row> execute(String brand, String type, List<Integer> sizeArray) {
        String querySQL = "SELECT p_brand, p_type, p_size, count(distinct ps_suppkey) as supplier_cnt "
                + "FROM partsupp,part "
                + "WHERE p_partkey = ps_partkey "
                + "AND p_brand <> '" + brand + "' " + "AND  p_type not like '" + type +"%' "
                + "AND p_size IN ( " + sizeArray.get(0) + ", " + sizeArray.get(1) + ", " + sizeArray.get(2) + ", " + sizeArray.get(3) + ", "
                + sizeArray.get(4) + ", " + sizeArray.get(5) + ", " + sizeArray.get(6) + ", " + sizeArray.get(7) + " ) "
                + "AND ps_suppkey NOT IN(  "
                + "SELECT s_suppkey FROM supplier "
                + "WHERE s_comment like '%Customer%Complaints%' ) "
                + "GROUP BY p_brand, p_type, p_size ORDER BY supplier_cnt desc, p_brand, p_type, p_size ";
        return spark.sql(querySQL).collectAsList();
    }
    
    /**
	 * Get the substitution parameter Size, selected randomly as a set of eight different values within [1 .. 50]
	 * @return array of randomly selected nations
	 */
    private List<Integer> getRandomSize()
    {
        List<Integer> sizeArray = new ArrayList<>(8);
        for(int i = 0; i < 8; i++){
            sizeArray.add(Utils.getRandomInt(1,50));}
        return sizeArray;

    }
}
