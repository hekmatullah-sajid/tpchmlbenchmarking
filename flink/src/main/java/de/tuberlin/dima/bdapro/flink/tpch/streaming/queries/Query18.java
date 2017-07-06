//package de.tuberlin.dima.bdapro.flink.tpch.streaming.queries;
//
//import java.util.List;
//
//import org.apache.flink.api.common.functions.FilterFunction;
//import org.apache.flink.api.common.functions.MapFunction;
//import org.apache.flink.api.common.operators.Order;
//import org.apache.flink.api.java.DataSet;
//import org.apache.flink.api.java.ExecutionEnvironment;
//import org.apache.flink.api.java.aggregation.Aggregations;
//import org.apache.flink.api.java.io.CsvReader;
//import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.api.java.tuple.Tuple4;
//import org.apache.flink.api.java.tuple.Tuple6;
//
//import de.tuberlin.dima.bdapro.flink.tpch.PathConfig;
//import de.tuberlin.dima.bdapro.flink.tpch.Utils;
//
//// Large Volume Customer Query (Q18)
//
//public class Query18 extends Query {
//
//	int quantity = Utils.getRandomInt(312, 315);
//	public Query18(final ExecutionEnvironment env, final String sf) {
//		super(env, sf);
//	}
//
//	public List<Tuple6<String, Integer, Integer, String, Double, Double>> execute(final int testQty) {
//		quantity = testQty;
//		return execute();
//	}
//
//	@Override
//	public List<Tuple6<String, Integer, Integer, String, Double, Double>> execute() {
//		//		The following tables and columns are needed for calculating results
//		//		customer(c_custkey, c_name) 
//		//		order(o_orderkey, o_custkey, o_totalprice, o_orderdate)
//		//		lineitem(l_orderkey, l_quantity) 
//		//		Result(c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, sum(l_quantity))
//
//		List<Tuple6<String, Integer, Integer, String, Double, Double>> out = null;
//
//		try{
//			DataSet<Tuple2<Integer, String>> customers = readCustomer();
//			DataSet<Tuple4<Integer, Integer, Double, String>> orders = readOrder();
//			DataSet<Tuple2<Integer, Double>> lineitems = readLineitem();
//
//			// Creating the inner query of where clause
//			DataSet<Tuple2<Integer, Double>> innerQ =
//					lineitems.groupBy(0).aggregate(Aggregations.SUM, 1).filter(filterLineitemInQ(quantity));
//
//			DataSet<Tuple6<String, Integer, Integer, String, Double, Double>> result =
//					orders.joinWithTiny(innerQ).where(0).equalTo(0).projectFirst(0,1,2,3).projectSecond(1)
//					// the result at this stage tmp(o_orderkey, o_custkey, o_totalprice, o_orderdate, sum(l_quantity)
//					.join(customers).where(1).equalTo(0).projectSecond(1).projectFirst(1,0,3, 2, 4)
//					//			result at this stage temp(c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, sum(l_quantity))
//					.join(lineitems).where(2).equalTo(0).projectFirst(0,1,2,3,4,5);
//
//			out = result.distinct().map(new MapFunction<Tuple6<String, Integer, Integer, String, Double, Double>, Tuple6<String, Integer, Integer, String, Double, Double>>() {
//				private static final long serialVersionUID = 1L;
//
//				@Override
//				public Tuple6<String, Integer, Integer, String, Double, Double> map(final Tuple6<String, Integer, Integer, String, Double, Double> value) throws Exception {
//					return Utils.keepOnlyTwoDecimals(value);
//				}
//			})
//
//					.sortPartition(4, Order.DESCENDING).sortPartition(3, Order.ASCENDING)
//					.first(100)
//					.collect();
//
//			return out;
//		}
//		catch(Exception e){
//			e.printStackTrace();
//		}
//
//		return null;
//	}
//
//	//	read customers
//	private DataSet<Tuple2<Integer, String>> readCustomer() {
//		final CsvReader source = getCSVReader(PathConfig.CUSTOMER);
//		return source.fieldDelimiter("|").includeFields("11000000")
//				.types(Integer.class, String.class);
//	}
//
//	//		read orders
//	private DataSet<Tuple4<Integer, Integer, Double, String>> readOrder() {
//		final CsvReader source = getCSVReader(PathConfig.ORDERS);
//		return source.fieldDelimiter("|").includeFields("110110000")
//				.types(Integer.class, Integer.class, Double.class, String.class);
//	}
//
//	// read lineitems
//	private DataSet<Tuple2<Integer, Double>> readLineitem() {
//		final CsvReader source = getCSVReader(PathConfig.LINEITEM);
//		return source.fieldDelimiter("|").includeFields("1000100000000000")
//				.types(Integer.class, Double.class);
//	}
//
//	//		Filter lineitems for inner query
//	private FilterFunction<Tuple2<Integer, Double>> filterLineitemInQ(final int qty) {
//		return (FilterFunction<Tuple2<Integer, Double>>) value -> 
//		(value.f1 > qty);
//	}
//
//
//}
