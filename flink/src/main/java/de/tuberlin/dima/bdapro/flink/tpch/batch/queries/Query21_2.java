package de.tuberlin.dima.bdapro.flink.tpch.batch.queries;

import java.util.List;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;

import de.tuberlin.dima.bdapro.flink.tpch.Utils.Nation;

public class Query21_2 extends Query {

	public Query21_2(BatchTableEnvironment env) {
		super(env);
		// TODO Auto-generated constructor stub
	}

	@Override
	public List<Tuple2<String, Long>> execute() {
		return execute(Nation.getRandomNation());
	}
	
	//and exists ( " + "select * from exY ) " + "and not exists (select * from exNY ) 

	public List<Tuple2<String, Long>> execute(String rndNation) {
		String querySQL = "SELECT s_name, count(*) as numwait from supplier, lineitem l1, orders, nation where s_suppkey = l1.l_suppkey "
				+ "and o_orderkey = l1.l_orderkey and o_orderstatus = 'F' and l1.l_receiptdate > l1.l_commitdate "
				+ "  NOT EXISTS (select * from exNY) "
				+ " and exists (select * from exY ) "
				+ " and s_nationkey = n_nationkey and n_name = '" + rndNation + "' group by s_name "
				+ "order by numwait desc, s_name limit 100";
		
		
//		Table supplier = env.scan("supplier");
//		Table l1 = env.scan("lineitem").filter("l_receiptdate > l_commitdate");
//		Table l2 = env.scan("lineitem");
//		Table l3 = env.scan("lineitem");
		
		String querySQExistL = "select * from lineitem l2, lineitem l1 where l2.l_orderkey = l1.l_orderkey and l2.l_suppkey <> l1.l_suppkey";
		Table exY = env.sql(querySQExistL);
		String querySQNoExistL = "select * from lineitem l3, lineitem l1 where l3.l_orderkey = l1.l_orderkey and l3.l_suppkey <> l1.l_suppkey "
				+ "and l3.l_receiptdate > l3.l_commitdate";
		Table exNY = env.sql(querySQNoExistL);
		
		
		env.registerTable("exY", exY);
		env.registerTable("exNY", exNY);


		
//		env.registerTable("l3", l3);

//		Table orders = env.scan("orders").filter("o_orderstatus = 'F'");
//		Table nation = env.scan("nation").filter("n_name = '" + rndNation + "'");
//		
//		Table res1 = supplier.join(l1).where("s_suppkey = l_suppkey")
//				.join(orders).where("o_orderkey = l_orderkey")
//				.join(nation).where("s_nationkey = n_nationkey")
//				.groupBy("s_name")
//				.select("s_name, count(*) as numwait")
//				.orderBy("numwait desc, s_name");
		
		Table res = env.sql(querySQL);

		
		try {
			return env.toDataSet(res, TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {
			})).map(new MapFunction<Tuple2<String, Long>, Tuple2<String, Long>>() {
				private static final long serialVersionUID = 1L;

				@Override
				public Tuple2<String, Long> map(final Tuple2<String, Long> value) throws Exception {
					return value;
				}
			}).collect();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return null;
	}

}
