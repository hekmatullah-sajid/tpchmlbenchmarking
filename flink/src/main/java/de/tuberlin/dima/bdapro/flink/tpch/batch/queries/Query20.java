package de.tuberlin.dima.bdapro.flink.tpch.batch.queries;

import java.time.LocalDate;
import java.util.List;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;

import de.tuberlin.dima.bdapro.flink.tpch.Utils;
import de.tuberlin.dima.bdapro.flink.tpch.Utils.Nation;

//Potential Part Promotion Query (Q20)
//The Potential Part Promotion Query identifies suppliers in a particular nation 
//having selected parts that may be candidates for a promotional offer.

public class Query20 extends Query {

	public Query20(BatchTableEnvironment env) {
		super(env);
		// TODO Auto-generated constructor stub
	}

	@Override
	public List<Tuple2<String, String>> execute() {
		return execute(Utils.getRandomColor(), LocalDate.parse(Utils.getRandomInt(1993, 1997) + "-01-01"), Nation.getRandomNation());
	}
	
	public List<Tuple2<String, String>> execute(String rndColor, LocalDate rndDate, String rndNation) {
		String querySQL = "SELECT s_name, s_address FROM supplier, nation WHERE s_suppkey in ("
				+ "SELECT ps_suppkey FROM partsupp WHERE ps_partkey IN (SELECT p_partkey FROM part WHERE p_name LIKE '" + rndColor + "%') "
				+ "and ps_availqty > ( SELECT 0.5 * SUM(l_quantity) FROM lineitem WHERE l_partkey = ps_partkey and l_suppkey = ps_suppkey "
				+ "and l_shipdate >= '" + rndDate.toString() + "' and l_shipdate < '" + rndDate.plusYears(1).toString() + "' ) ) and s_nationkey = n_nationkey "
				+ "and n_name = '" + rndNation +"' ORDER BY s_name";
		
		Table res = env.sql(querySQL);
		
		try {
			return env.toDataSet(res, TypeInformation.of(new TypeHint<Tuple2<String, String>>() {})).collect();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
}
