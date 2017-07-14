package de.tuberlin.dima.bdapro.spark.tpch.batch.queries;

import java.time.LocalDate;
import java.util.List;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import de.tuberlin.dima.bdapro.spark.tpch.Utils;

/**
 * Created by seema on 07/06/2017.
 */
public class Query14 extends Query {
    public Query14(SparkSession spark) {
        super(spark);
    }

    @Override
    public List<Row> execute() {
        return execute(getRandomDate());
    }

    public List<Row> execute(LocalDate date) {
        String querySQL = "SELECT 100.00 * sum(case "
                + "WHEN p_type like 'PROMO%' "
                + "THEN l_extendedprice*(1-l_discount) "
                + "ELSE 0 "
                + "end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue "
                + "FROM lineitem,part "
                + "WHERE l_partkey = p_partkey "
                + "and l_shipdate >=  '" + date.toString() + "' "
                + "and l_shipdate < '" +  date.plusMonths(1).toString() + "' ";
        return spark.sql(querySQL).collectAsList();
    }
    private LocalDate getRandomDate()
    {
        return LocalDate.of(Utils.getRandomInt(1992,1997), Utils.getRandomInt(0,12), 1);
    }
}
