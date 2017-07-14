package de.tuberlin.dima.bdapro.flink.tpch.batch.queries;

import java.util.List;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;

import de.tuberlin.dima.bdapro.flink.tpch.Utils.Nation;

public class Query21 extends Query {

	public Query21(BatchTableEnvironment env) {
		super(env);
		// TODO Auto-generated constructor stub
	}

	@Override
	public List<Tuple2<String, Long>> execute() {
		return execute(Nation.getRandomNation());
	}
	public List<Tuple2<String, Long>> execute(String rndNation) {
		String querySQL = "SELECT s_name, count(*) as numwait from supplier, lineitem l1, orders, nation where s_suppkey = l1.l_suppkey "
				+ "and o_orderkey = l1.l_orderkey and o_orderstatus = 'F' and l1.l_receiptdate > l1.l_commitdate and exists ( "
				+ "select * from lineitem l2 where l1.l_orderkey = l2.l_orderkey and l1.l_suppkey <> l2.l_suppkey ) "
				+ "and not exists (select * from lineitem l3 where l1.l_orderkey = l3.l_orderkey and l1.l_suppkey <> l3.l_suppkey "
				+ "and l3.l_receiptdate > l3.l_commitdate ) and s_nationkey = n_nationkey and n_name = '" + rndNation + "' group by s_name "
				+ "order by numwait desc, s_name limit 100";
		
		Table res = env.sql(querySQL);
		
		try {
			return env.toDataSet(res, TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {})).collect();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return null;
	}

}
