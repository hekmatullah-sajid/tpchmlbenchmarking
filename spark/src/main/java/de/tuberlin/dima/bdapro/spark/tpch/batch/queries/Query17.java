package de.tuberlin.dima.bdapro.spark.tpch.batch.queries;

import java.util.List;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import de.tuberlin.dima.bdapro.spark.tpch.config.Utils;

/**
 * Small-Quantity-Order Revenue Query (Q17), TPC-H Benchmark Specification page 57 http://www.tpc.org/tpc_documents_current_versions/pdf/tpc-h_v2.17.2.pdf). 
 * @author Seema Narasimha Swamy
 *
 */
public class Query17 extends Query {
    public Query17(SparkSession spark) {
        super(spark);
    }

    /**
	 * Find the random values and pass it to the execute method (with parameter).
     */
    @Override
    public List<Row> execute() {
        return execute(Utils.getRandomBrand(), Utils.getRandomContainer());
    }
    
    /**
     * Executes Query17 of TPC-H and returns the result.
     * @param brand 'Brand#MN' where MN is a two character string representing two numbers randomly and independently selected within [1 .. 5] by respectively utility function;
     * @param container is randomly selected within the lists of CONTAINERS_SYL1 and CONTAINERS_SYL2
     * @return the result of the query
     */
    public List<Row> execute(String brand, String container) {
        String querySQL = "SELECT sum(l_extendedprice) / 7.0 as avg_yearly "
                + "FROM lineitem, part "
                + "WHERE p_partkey = l_partkey and p_brand = '" + brand + "' "
                + "and p_container = '" + container + "' "
                + "and l_quantity < ( "
                + "select 0.2 * avg(l_quantity) from lineitem "
                + "where l_partkey = p_partkey ) ";
        return spark.sql(querySQL).collectAsList();
    }
}
