package de.tuberlin.dima.bdapro.spark.tpch.batch.queries;

import java.util.List;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import de.tuberlin.dima.bdapro.spark.tpch.Utils;

/**
 * Created by seema on 07/06/2017.
 */
public class Query17 extends Query {
    public Query17(SparkSession spark) {
        super(spark);
    }

    @Override
    public List<Row> execute() {
        return execute(Utils.getRandomBrand(), Utils.getRandomContainer());
    }
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
