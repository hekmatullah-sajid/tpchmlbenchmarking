package de.tuberlin.dima.bdapro.spark.tpch.batch.queries;

import java.util.List;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import de.tuberlin.dima.bdapro.spark.tpch.Utils;

/**
 * Created by seema on 07/06/2017.
 */
public class Query13 extends Query {
    public Query13(SparkSession spark) {
        super(spark);
    }

    @Override
    public List<Row> execute() {
        return execute(Utils.getRandomWord1(), Utils.getRandomWord2());
    }
    public List<Row> execute(String word1, String word2) {
        return spark.sql("SELECT c_count, count(*) as custdist "
                + "FROM ( "
                + "SELECT c_custkey, count(o_orderkey) as c_count "
                + "FROM customer left outer join orders on c_custkey = o_custkey "
                + "and o_comment not like '%" + word1 + "%" + word2 + "%' "
                + "GROUP BY c_custkey) "
                + "GROUP BY c_count ORDER BY custdist desc, c_count desc ").collectAsList();
    }

}
