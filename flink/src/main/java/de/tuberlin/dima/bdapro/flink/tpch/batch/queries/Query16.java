package de.tuberlin.dima.bdapro.flink.tpch.batch.queries;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;

import de.tuberlin.dima.bdapro.flink.tpch.batch.config.Utils;

/**
 * Parts/Supplier Relationship Query (Q16), TPC-H Benchmark Specification page
 * 55 http://www.tpc.org/tpc_documents_current_versions/pdf/tpc-h_v2.17.2.pdf).
 * 
 * @author Seema Narasimha Swamy
 *
 */
public class Query16 extends Query {
	public Query16(BatchTableEnvironment env) {
		super(env);
	}

	/**
	 * Find the random values and pass it to the execute method (with
	 * parameter).
	 */
	@Override
	public List<Tuple4<String, String, Integer, Long>> execute() {
		return execute(Utils.getRandomBrand(), Utils.getRandomType(), getRandomSize());
	}

	/**
	 * Executes Query16 of TPC-H and returns the result.
	 * 
	 * The DISTINCT SQL operator was not supported by Flink, the distinct
	 * operator and query is performed using Table API. The PARTSUPP table is
	 * loaded into Flink Table and the duplication is eliminated using the
	 * "distinct()" function of Table API, and the result is registered in
	 * BatchTableEnvironment. The query is then performed without the
	 * BatchTableEnvironment.
	 * 
	 * @param brand
	 *            Brand#MN where M and N are two single character strings
	 *            representing two numbers randomly and independently selected
	 *            within [1 .. 5].
	 * @param type
	 *            is made of the first 2 syllables of a string randomly selected
	 *            from the lists TYPE_SYL1, TYPE_SYL2 and TYPE_SYL3.
	 * @param sizeArray
	 *            is randomly selected as a set of eight different values within
	 *            [1 .. 50].
	 * @return result of the query
	 */
	public List<Tuple4<String, String, Integer, Long>> execute(String brand, String type, List<Integer> sizeArray) {
		Integer size1 = sizeArray.get(0);
		Integer size2 = sizeArray.get(1);
		Integer size3 = sizeArray.get(2);
		Integer size4 = sizeArray.get(3);
		Integer size5 = sizeArray.get(4);
		Integer size6 = sizeArray.get(5);
		Integer size7 = sizeArray.get(6);
		Integer size8 = sizeArray.get(7);

		Table partsupp_mod = env.scan("partsupp").select("ps_partkey,ps_suppkey").distinct();
		env.registerTable("partsupp_mod", partsupp_mod);

		String SQLQuery = "SELECT p_brand, p_type, p_size, count(ps_suppkey) as supplier_cnt "
				+ "FROM partsupp_mod, part " + "WHERE p_partkey = ps_partkey " + "AND p_brand <> '" + brand + "' "
				+ "AND  p_type not like '" + type + "%' " + "AND p_size IN ( " + size1 + ", " + size2 + ", " + size3
				+ ", " + size4 + ", " + size5 + ", " + size6 + ", " + size7 + ", " + size8 + " ) " + "AND NOT EXISTS(  "
				+ "SELECT s_suppkey FROM supplier "
				+ "WHERE s_comment like '%Customer%Complaints%' AND s_suppkey = ps_suppkey) "
				+ "GROUP BY p_brand, p_type, p_size ORDER BY supplier_cnt desc, p_brand, p_type, p_size ";

		Table res = env.sql(SQLQuery);

		/*
		 * Collect and return the result.
		 */
		try {
			return env.toDataSet(res, TypeInformation.of(new TypeHint<Tuple4<String, String, Integer, Long>>() {
			})).collect();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * Get the substitution parameter Size, selected randomly as a set of eight
	 * different values within [1 .. 50]
	 * 
	 * @return array of randomly selected nations
	 */
	private List<Integer> getRandomSize() {
		List<Integer> sizeArray = new ArrayList<>(8);
		for (int i = 0; i < 8; i++) {
			sizeArray.add(Utils.getRandomInt(1, 50));
		}
		return sizeArray;

	}
}
