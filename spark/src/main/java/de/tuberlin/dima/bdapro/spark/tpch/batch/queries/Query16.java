package de.tuberlin.dima.bdapro.spark.tpch.batch.queries;

import de.tuberlin.dima.bdapro.spark.tpch.Utils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by seema on 07/06/2017.
 */
public class Query16 extends Query {
    public Query16(SparkSession spark) {
        super(spark);
    }

    @Override
    public List<Row> execute() {
        return execute(Utils.getRandomBrand(),Utils.getRandomType(), getRandomSize());
    }
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
    private List<Integer> getRandomSize()
    {
        List<Integer> sizeArray = new ArrayList<>(8);
        for(int i = 0; i < 8; i++){
            sizeArray.add(Utils.getRandomInt(1,50));}
        return sizeArray;

    }
}
