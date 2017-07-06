package de.tuberlin.dima.bdapro.flink.tpch.batch.queries;

import java.time.LocalDate;
import java.util.List;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;

import de.tuberlin.dima.bdapro.flink.tpch.Utils;

public class Query12 extends Query {

	public Query12(BatchTableEnvironment env) {
		super(env);
	}

	@Override
	public List<Tuple3<String, Integer, Integer>> execute() {
		return execute(Utils.getRandomShipmode(), Utils.getRandomShipmode(), LocalDate.parse(Utils.getRandomInt(1993, 1997) + "-01-01"));
	}
	public List<Tuple3<String, Integer, Integer>> execute(String rndShipmode1, String rndShipmode2, LocalDate rndDate) {
		String querySQL = "SELECT l_shipmode, sum(case when o_orderpriority ='1-URGENT' or o_orderpriority ='2-HIGH' then 1 else 0 end) as high_line_count, "
				+ "sum(case when o_orderpriority <> '1-URGENT' and o_orderpriority <> '2-HIGH' then 1 else 0 end) as low_line_count "
				+ "from orders, lineitem where o_orderkey = l_orderkey and l_shipmode in ('" + rndShipmode1 + "', '" + rndShipmode2 + "') "
				+ "and l_commitdate < l_receiptdate and l_shipdate < l_commitdate and "
				+ "l_receiptdate >= '" + rndDate.toString() + "' and l_receiptdate < '" + rndDate.plusYears(1).toString() + "' "
				+ "group by l_shipmode order by l_shipmode"; 
		Table res = env.sql(querySQL);
		
		try {
			return env.toDataSet(res, TypeInformation.of(new TypeHint<Tuple3<String, Integer, Integer>>() {
            })).collect();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return null;
	}

}
