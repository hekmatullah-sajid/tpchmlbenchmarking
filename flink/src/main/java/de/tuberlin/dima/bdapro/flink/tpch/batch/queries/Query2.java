package de.tuberlin.dima.bdapro.flink.tpch.batch.queries;

import java.util.List;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;

import de.tuberlin.dima.bdapro.flink.tpch.batch.config.Utils;
import de.tuberlin.dima.bdapro.flink.tpch.batch.config.Utils.Nation;

/**
 * Minimum Cost Supplier Query (Q2), TPC-H Benchmark Specification page 30
 * http://www.tpc.org/tpc_documents_current_versions/pdf/tpc-h_v2.17.2.pdf).
 * 
 * @author Hekmatullah Sajid
 *
 */
public class Query2 extends Query {

	public Query2(final BatchTableEnvironment env) {
		super(env);

	}

	/**
	 * Find the random values and pass it to the execute method (with
	 * parameter).
	 */
	@Override
	public List<Tuple8<Double, String, String, Integer, String, String, String, String>> execute() {
		return execute(Utils.getRandomTypeSyl3(), Utils.getRandomInt(1993, 1997), Nation.getRandomRegion());
	}

	/**
	 * Executes Query2 of TPC-H and returns the result. The SQL of the query was
	 * not supported by Flink due to having complex inner query. The inner and
	 * outer queries are executed separately using Flink Table API. Finally the
	 * results of both queries are joined to get the actual query result.
	 * 
	 * @param pType
	 *            is randomly selected within [1. 50];
	 * @param pSize
	 *            is randomly selected within the list Syllable 3
	 * @param rRegion
	 *            is randomly selected within the list of Regions
	 * @return result of the query
	 * 
	 */
	public List<Tuple8<Double, String, String, Integer, String, String, String, String>> execute(String pType,
			int pSize, String rRegion) {
		Table supplier = env.scan("supplier");
		Table partsupp = env.scan("partsupp");
		Table part = env.scan("part").filter("p_size = " + pSize).filter("LIKE(p_type, '%" + pType + "%')");
		Table nation = env.scan("nation");
		Table region = env.scan("region").filter("r_name = '" + rRegion + "'");

		Table innerQuery = part.join(partsupp).where("p_partkey = ps_partkey").join(supplier)
				.where("s_suppkey = ps_suppkey").join(nation).where("s_nationkey = n_nationkey").join(region)
				.where("n_regionkey = r_regionkey").groupBy("p_partkey").select("min(ps_supplycost) as mincost");

		Table outterQuery = part.join(partsupp).where("p_partkey = ps_partkey").join(supplier)
				.where("s_suppkey = ps_suppkey").join(nation).where("s_nationkey = n_nationkey").join(region)
				.where("n_regionkey = r_regionkey");

		Table res = outterQuery.join(innerQuery).where("ps_supplycost = mincost")
				.orderBy("s_acctbal, n_name, s_name, p_partkey").limit(100)
				.select(" s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment ");

		/*
		 * Drop more than two decimal values in double values. And return the
		 * result
		 */
		try {
			return env.toDataSet(res, TypeInformation
					.of(new TypeHint<Tuple8<Double, String, String, Integer, String, String, String, String>>() {
					}))
					.map(new MapFunction<Tuple8<Double, String, String, Integer, String, String, String, String>, Tuple8<Double, String, String, Integer, String, String, String, String>>() {
						private static final long serialVersionUID = 1L;

						@Override
						public Tuple8<Double, String, String, Integer, String, String, String, String> map(
								final Tuple8<Double, String, String, Integer, String, String, String, String> value)
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
