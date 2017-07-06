package de.tuberlin.dima.bdapro.flink.tpch.batch.queries;

import java.util.List;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;

import de.tuberlin.dima.bdapro.flink.tpch.Utils;

public class Query18 extends Query {

	public Query18(BatchTableEnvironment env) {
		super(env);
	}

	@Override
	public List<Tuple6<String, Integer, Integer, String, Double, Double>> execute() {
		return execute(Utils.getRandomInt(312, 315));
	}
	
	public List<Tuple6<String, Integer, Integer, String, Double, Double>> execute(int rndQty) {
		String querySQL = "SELECT c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, sum(l_quantity) from customer, orders, lineitem "
				+ "WHERE o_orderkey in (SELECT l_orderkey from lineitem group by l_orderkey having sum(l_quantity) > " + rndQty +") "
				+ "and c_custkey = o_custkey and o_orderkey = l_orderkey "
				+ "GROUP BY c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice "
				+ "ORDER BY o_totalprice desc, o_orderdate limit 100";
		
		Table res = env.sql(querySQL);
		
		try{
			return env.toDataSet(res, TypeInformation.of(new TypeHint<Tuple6<String, Integer, Integer, String, Double, Double>>() {
			})).map(new MapFunction<Tuple6<String, Integer, Integer, String, Double, Double>, Tuple6<String, Integer, Integer, String, Double, Double>>() {
				private static final long serialVersionUID = 1L;

				@Override
				public Tuple6<String, Integer, Integer, String, Double, Double> map(final Tuple6<String, Integer, Integer, String, Double, Double> value) throws Exception {
					return Utils.keepOnlyTwoDecimals(value);
				}
			}).collect();
		} 
		catch (Exception e) {
			e.printStackTrace();
		}
		
		return null;
	}

}
 