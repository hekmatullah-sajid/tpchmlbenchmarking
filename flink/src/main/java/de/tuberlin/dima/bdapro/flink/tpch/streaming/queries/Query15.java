//package de.tuberlin.dima.bdapro.flink.tpch.streaming.queries;
//
//import java.time.LocalDate;
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
//import org.apache.flink.api.java.tuple.Tuple5;
//
//import de.tuberlin.dima.bdapro.flink.tpch.PathConfig;
//import de.tuberlin.dima.bdapro.flink.tpch.Utils;
//
//// Top Supplier Query (Q15)
//
//public class Query15 extends Query {
//	LocalDate randDate = LocalDate.parse(Utils.getRandomInt(1993, 1997) + "-01-01");
//
//	public Query15(final ExecutionEnvironment env, final String sf) {
//		super(env, sf);
//		// TODO Auto-generated constructor stub
//	}
//
//	public List<Tuple5<Integer, String, String, String, Double>> execute(final String testDate) {
//		randDate = LocalDate.parse(testDate);
//		return execute();
//	}
//
//	@Override
//	public List<Tuple5<Integer, String, String, String, Double>> execute() {
//		// TODO Auto-generated method stub
//
//		try{
//			// lineitem(l_suppkey, l_extendedprice, l_discount, l_shipdate) 
//			final DataSet<Tuple4<Integer, Double, Double, String>> lineitem = readLineitem();
//			// Supplier(s_suppkey, s_name, s_address, s_phone)
//			final DataSet<Tuple4<Integer, String, String, String>> suppliers = readSupplier();
//
//			final DataSet<Tuple4<Integer, Double, Double, String>> lineitemFiltered = lineitem.filter(filterLineitems(randDate));
//
//			// Result(s_suppkey, s_name, s_address, s_phone, totalRevenue)
//
//			final DataSet<Tuple2<Integer, Double>> viewRevenue = 
//					lineitemFiltered.map(new MapFunction<Tuple4<Integer, Double, Double, String>, Tuple2<Integer, Double>>() {
//						private static final long serialVersionUID = 1L;
//
//						@Override
//						public Tuple2<Integer, Double> map(final Tuple4<Integer, Double, Double, String> value) throws Exception {
//							return new Tuple2<Integer, Double>(value.f0, value.f1 * (1 - value.f2));
//						}
//					}).groupBy(0).aggregate(Aggregations.SUM, 1).map(new MapFunction<Tuple2<Integer, Double>, Tuple2<Integer, Double>>() {
//						private static final long serialVersionUID = 1L;
//
//						@Override
//						public Tuple2<Integer, Double> map(final Tuple2<Integer, Double> value) throws Exception {
//							return Utils.keepOnlyTwoDecimals(value);
//						}
//					});
//
//			final DataSet<Tuple2<Integer, Double>> maxRevenue = viewRevenue.maxBy(1);
//			//Perform join between view and supplier
//			DataSet<Tuple5<Integer, String, String, String, Double>> reuslt =
//					suppliers.join(viewRevenue).where(0).equalTo(0).projectFirst(0,1,2,3).projectSecond(1)
//					.join(maxRevenue).where(4).equalTo(1).projectFirst(0,1,2,3,4);
//
//			// Create output
//			final List<Tuple5<Integer, String, String, String, Double>> out = 
//					reuslt.map(new MapFunction<Tuple5<Integer, String, String, String, Double>, 
//							Tuple5<Integer, String, String, String, Double>>() {
//						/**
//						 * 
//						 */
//						private static final long serialVersionUID = 1L;
//
//						@Override
//						public Tuple5<Integer, String, String, String, Double> map(final Tuple5<Integer, String, String, String, Double> value) throws Exception {
//							return value;
//						}
//					}).sortPartition(0, Order.ASCENDING).collect();
//
//			return out;
//		}
//		catch (final Exception e) {
//			e.printStackTrace();
//		}
//
//		return null;
//	}
//
//	// read lineitems
//	private DataSet<Tuple4<Integer, Double, Double, String>> readLineitem() {
//		final CsvReader source = getCSVReader(PathConfig.LINEITEM);
//		return source.fieldDelimiter("|").includeFields("0010011000100000")
//				.types(Integer.class, Double.class, Double.class, String.class);
//	}
//
//	// read suppliers
//	private DataSet<Tuple4<Integer, String, String, String>> readSupplier() {
//		final CsvReader source = getCSVReader(PathConfig.SUPPLIER);
//		return source.fieldDelimiter("|").includeFields("1110100")
//				.types(Integer.class, String.class, String.class, String.class);
//	}
//
//	//filter lineitmes
//	private FilterFunction<Tuple4<Integer, Double, Double, String>> filterLineitems(final LocalDate date) {
//		return (FilterFunction<Tuple4<Integer, Double, Double, String>>) value -> 
//		(LocalDate.parse(value.f3).isEqual(date)
//				|| LocalDate.parse(value.f3).isAfter(date))
//		&& (LocalDate.parse(value.f3).isBefore(date.plusMonths(3)));
//	}
//
//}
