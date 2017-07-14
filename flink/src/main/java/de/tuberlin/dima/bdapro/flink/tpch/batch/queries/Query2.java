package de.tuberlin.dima.bdapro.flink.tpch.batch.queries;

import java.util.List;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;

import de.tuberlin.dima.bdapro.flink.tpch.Utils;
import de.tuberlin.dima.bdapro.flink.tpch.Utils.Nation;


public class Query2 extends Query {
	

	public Query2(final BatchTableEnvironment env) {
		super(env);
		
	}

	@Override
	public List<Tuple8<Double, String, String, Integer, String, String, String, String>> execute() {
		return execute(Utils.getRandomTypeSyl3(), Utils.getRandomInt(1993, 1997), Nation.getRandomRegion());
	}
	
	public List<Tuple8<Double, String, String, Integer, String, String, String, String>> execute(String pType, int pSize, String rRegion) {
//		String SQLQuery = "SELECT s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment "
//				+ "FROM part, supplier, partsupp, nation, region "
//				+ "WHERE p_partkey = ps_partkey and s_suppkey = ps_suppkey and p_size = " + pSize + " "
//				+ "and p_type like '%" + pType + "' and s_nationkey = n_nationkey and n_regionkey = r_regionkey "
//				+ "and r_name = '" + rRegion + "' ";
////						+ "and ps_supplycost = (SELECT min(ps_supplycost) from partsupp, supplier, nation, region "
////				+ "where p_partkey = ps_partkey and s_suppkey = ps_suppkey and s_nationkey = n_nationkey and "
////				+ "n_regionkey = r_regionkey and r_name = '" + rRegion + "' ) ";
//				//+ "order by s_acctbal desc, n_name, s_name, p_partkey";
//		System.out.println(SQLQuery);
//		Table res = env.sql(SQLQuery);
		
		
		Table supplier = env.scan("supplier");
		Table partsupp = env.scan("partsupp");
		Table part = env.scan("part").filter("p_size = " + pSize )
				.filter("LIKE(p_type, '%" + pType + "%')"); 
		Table nation = env.scan("nation");
		Table region = env.scan("region").filter("r_name = '" + rRegion + "'");
		
		Table innerQuery = part.join(partsupp).where("p_partkey = ps_partkey")
				.join(supplier).where("s_suppkey = ps_suppkey")
				.join(nation).where("s_nationkey = n_nationkey")
				.join(region).where("n_regionkey = r_regionkey")
				.groupBy("p_partkey")
				.select("min(ps_supplycost) as mincost");
		
		Table outterQuery =  part.join(partsupp).where("p_partkey = ps_partkey")
				.join(supplier).where("s_suppkey = ps_suppkey")
				.join(nation).where("s_nationkey = n_nationkey")
				.join(region).where("n_regionkey = r_regionkey");
		
		Table res = outterQuery.join(innerQuery).where("ps_supplycost = mincost")
				.orderBy("s_acctbal, n_name, s_name, p_partkey")
				.limit(100)
				.select(" s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment ");
		
		try{
			return env.toDataSet(res, TypeInformation.of(new TypeHint<Tuple8<Double, String, String, Integer, String, String, String, String>>() {
			})).map(new MapFunction<Tuple8<Double, String, String, Integer, String, String, String, String>, Tuple8<Double, String, String, Integer, String, String, String, String>>() {
				private static final long serialVersionUID = 1L;

				@Override
				public Tuple8<Double, String, String, Integer, String, String, String, String> map(final Tuple8<Double, String, String, Integer, String, String, String, String> value) throws Exception {
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
