package de.tuberlin.dima.bdapro.flink.tpch.batch.queries;

import de.tuberlin.dima.bdapro.flink.tpch.Utils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;

import java.time.LocalDate;
import java.util.List;
import java.util.Random;

/**
 * Created by seema on 05/06/2017.
 */
public class Query4 extends Query {
    public Query4(final BatchTableEnvironment env) {
        super(env);
    }
    @Override
    public List<Tuple2<String, Long>> execute() {
        return  execute(getRandomDate());
    }

    public List<Tuple2<String, Long>> execute(LocalDate rndDate) {

        String SQLQuery = "SELECT o_orderpriority,count( * ) as order_count "
                + "FROM orders "
                + "WHERE o_orderdate >= '" + rndDate.toString() + "' and "
                + "o_orderdate < '" + rndDate.plusMonths(3).toString() + "' "
                + "and exists ( "
                + "SELECT * FROM lineitem "
                + "WHERE l_orderkey = o_orderkey and  l_commitdate < l_receiptdate ) "
                + "GROUP BY o_orderpriority ORDER BY o_orderpriority";

        Table res = env.sql(SQLQuery);

        try {
            return env.toDataSet(res, TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {
            })).collect();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
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
}
