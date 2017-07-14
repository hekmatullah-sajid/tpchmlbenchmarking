package de.tuberlin.dima.bdapro.flink.tpch.batch.queries;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Random;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;

import de.tuberlin.dima.bdapro.flink.tpch.Utils;

/**
 * Created by seema on 30/05/2017.
 */
public class Query3 extends Query{
    public Query3(final BatchTableEnvironment env) {
        super(env);
    }

    @Override
    public List<Tuple4<Integer, Double, String, Integer>> execute() {
        String segment =  Utils.getRandomSegment();
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        return execute(segment, getRandomDate(), dateTimeFormatter);
    }

    private LocalDate getRandomDate()
    	{
		Random rand = new Random();
		return LocalDate.of(1995, 3, rand.nextInt((31 - 1) + 1) + 1);
	}
    public List<Tuple4<Integer, Double, String, Integer>> execute(
            String segment, LocalDate date, DateTimeFormatter dateTimeFormatter) {

        String SQLQuery = "select l_orderkey, sum(l_extendedprice * (1-l_discount)) as revenue, o_orderdate, o_shipriority "
                + "FROM customer, orders, lineitem "
                + "WHERE c_mktsegment = '" + segment +"' and "
                + "c_custkey = o_custkey and  l_orderkey = o_orderkey and "
                + "o_orderdate < '" + date.toString() +"' and "
                + "l_shipdate > '" + date.toString() +"' "
                + "GROUP BY l_orderkey, o_orderdate, o_shipriority ORDER BY revenue desc, o_orderdate";

        Table res = env.sql(SQLQuery);


        try {
            return env.toDataSet(res, TypeInformation.of
                    (new TypeHint<Tuple4<Integer, Double, String, Integer>>(){}))
                    .map(new MapFunction<Tuple4<Integer, Double, String, Integer>, Tuple4<Integer, Double, String, Integer>>() {
                        private static final long serialVersionUID = 1L;

                        @Override
                        public Tuple4<Integer, Double, String, Integer> map(
                                final Tuple4<Integer, Double, String, Integer> value)
                                throws Exception {
                            return Utils.keepOnlyTwoDecimals(value);
                        }
                    }).collect();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
