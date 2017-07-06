package de.tuberlin.dima.bdapro.flink.tpch.batch.queries;

import java.time.LocalDate;
import java.util.List;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;

import de.tuberlin.dima.bdapro.flink.tpch.Utils;

//Top Supplier Query (Q15)

public class Query15 extends Query {

	public Query15(BatchTableEnvironment env) {
		super(env);
		// TODO Auto-generated constructor stub
	}

	@Override
	public List<Tuple5<Integer, String, String, String, Double>> execute() {
		return execute(LocalDate.parse(Utils.getRandomInt(1993, 1997) + "-01-01"));
	}
	
	public List<Tuple5<Integer, String, String, String, Double>> execute(final LocalDate rndDate) {
		String viewSQL = "SELECT l_suppkey as supplier_no, SUM(l_extendedprice * (1 - l_discount)) as total_revenue FROM lineitem WHERE l_shipdate >= '" + rndDate.toString() +"' "
				+ "and l_shipdate < '" + rndDate.plusMonths(3).toString() + "' "
				+ "GROUP BY l_suppkey";
		Table viewRevenue = env.sql(viewSQL);
		
		env.registerTable("viewrevenue", viewRevenue);
		
		String resSQL = "SELECT s_suppkey, s_name, s_address, s_phone, total_revenue FROM supplier, viewrevenue "
				+ "WHERE s_suppkey = supplier_no and total_revenue = ( "
				+ "SELECT max(total_revenue) from viewrevenue ) ORDER BY s_suppkey";
		
		Table res = env.sql(resSQL);
		
		try{
			return env.toDataSet(res, TypeInformation.of(new TypeHint<Tuple5<Integer, String, String, String, Double>>() {
			})).map(new MapFunction<Tuple5<Integer, String, String, String, Double>, Tuple5<Integer, String, String, String, Double>>() {
				private static final long serialVersionUID = 1L;

				@Override
				public Tuple5<Integer, String, String, String, Double> map(final Tuple5<Integer, String, String, String, Double> value) throws Exception {
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
