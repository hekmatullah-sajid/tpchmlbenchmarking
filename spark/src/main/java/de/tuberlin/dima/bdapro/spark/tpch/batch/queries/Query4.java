package de.tuberlin.dima.bdapro.spark.tpch.batch.queries;

import java.time.LocalDate;
import java.util.List;
import java.util.Random;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Created by seema on 06/06/2017.
 */
public class Query4 extends Query {
    public Query4(final SparkSession spark) {
        super(spark);
    }

    @Override
    public List<Row> execute() {
        return execute(getRandomDate());
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
}
