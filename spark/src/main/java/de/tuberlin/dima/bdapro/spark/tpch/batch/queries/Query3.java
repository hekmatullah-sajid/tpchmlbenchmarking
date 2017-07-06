package de.tuberlin.dima.bdapro.spark.tpch.batch.queries;

import de.tuberlin.dima.bdapro.spark.tpch.Utils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Random;

/**
 * Created by seema on 06/06/2017.
 */
public class Query3 extends Query{

    public Query3(final SparkSession spark) {
        super(spark);
    }

    @Override
    public List<Row> execute() {
        String segment =  Utils.getRandomSegment();
        LocalDate date = getRandomDate();
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        return execute(segment, getRandomDate(), dateTimeFormatter);
    }
    private LocalDate getRandomDate()
    {
        Random rand = new Random();
        return LocalDate.of(1995, 3, rand.nextInt((31 - 1) + 1) + 1);
    }
    public List<Row> execute(String segment, LocalDate date, DateTimeFormatter dateTimeFormatter) {

        return spark.sql("select l_orderkey, sum(l_extendedprice * (1-l_discount)) as revenue, o_orderdate, o_shipriority "
                + "FROM customer, orders, lineitem "
                + "WHERE c_mktsegment = '" + segment +"' and "
                + "c_custkey = o_custkey and  l_orderkey = o_orderkey and "
                + "o_orderdate < '" + date.toString() +"' and "
                + "l_shipdate > '" + date.toString() +"' "
                + "GROUP BY l_orderkey, o_orderdate, o_shipriority ORDER BY revenue desc, o_orderdate").collectAsList();

    }
}

