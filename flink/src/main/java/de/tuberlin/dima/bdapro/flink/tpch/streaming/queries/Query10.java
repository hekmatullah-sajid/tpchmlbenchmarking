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
//import org.apache.flink.api.java.tuple.Tuple3;
//import org.apache.flink.api.java.tuple.Tuple4;
//import org.apache.flink.api.java.tuple.Tuple7;
//import org.apache.flink.api.java.tuple.Tuple8;
//
//import de.tuberlin.dima.bdapro.flink.tpch.PathConfig;
//import de.tuberlin.dima.bdapro.flink.tpch.Utils;
//
//public class Query10 extends Query {
//
//	public Query10(final ExecutionEnvironment env, final String sf) {
//		super(env, sf);
//	}
//
//	@Override
//	public List<Tuple8<Integer, String, Double, Double, String, String, String, String>> execute() {
//		return execute(getRandomDate());
//	}
//
//	public List<Tuple8<Integer, String, Double, Double, String, String, String, String>> execute(final String date) {
//		LocalDate randDate = LocalDate.parse(date);
//		DataSet<Tuple2<Integer, Double>> lineitem = readLineitem()
//				.filter(new FilterFunction<Tuple4<Integer, Double, Double, String>>() {
//					private static final long serialVersionUID = 1L;
//
//					@Override
//					public boolean filter(final Tuple4<Integer, Double, Double, String> value) throws Exception {
//						return value.f3.equals("R");
//					}
//				}).map(new MapFunction<Tuple4<Integer, Double, Double, String>, Tuple2<Integer, Double>>() {
//					private static final long serialVersionUID = 1L;
//
//					@Override
//					public Tuple2<Integer, Double> map(final Tuple4<Integer, Double, Double, String> value)
//							throws Exception {
//						return Utils.keepOnlyTwoDecimals(new Tuple2<Integer, Double>(value.f0, value.f1 * (1 - value.f2)));
//					}
//				});
//		DataSet<Tuple2<Integer, String>> nation = readNation();
//		DataSet<Tuple2<Integer, Integer>> orders = readOrders()
//				.filter(new FilterFunction<Tuple3<Integer, Integer, String>>() {
//					private static final long serialVersionUID = 1L;
//
//					@Override
//					public boolean filter(final Tuple3<Integer, Integer, String> value) throws Exception {
//						LocalDate currDate = LocalDate.parse(value.f2);
//						return (currDate.isAfter(randDate) || currDate.isEqual(randDate))
//								&& currDate.isBefore(randDate.plusMonths(3));
//					}
//				}).project(0, 1);
//		DataSet<Tuple7<Integer, String, String, Integer, String, Double, String>> customer = readCustomer();
//
//		// join customer with nations
//		DataSet<Tuple7<Integer, String, Double, String, String, String, String>> customerWithNations = customer
//				.joinWithTiny(nation).where(3).equalTo(0).projectFirst(0, 1, 5).projectSecond(1).projectFirst(2, 4, 6);
//
//		// join oders with lineitems
//		DataSet<Tuple2<Integer, Double>> orderWithLineitem = orders.joinWithHuge(lineitem).where(0).equalTo(0)
//				.projectFirst(1).projectSecond(1);
//
//		// join customer with revenue
//		try {
//			DataSet<Tuple8<Integer, String, Double, Double, String, String, String, String>> result = customerWithNations
//					.join(orderWithLineitem).where(0).equalTo(0).projectFirst(0, 1).projectSecond(1)
//					.projectFirst(2, 3, 4, 5, 6);
//
//			return result.groupBy(0, 1, 3, 6, 4, 5, 7).aggregate(Aggregations.SUM, 2).sortPartition(2, Order.DESCENDING)
//					.collect();
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//		return null;
//	}
//
//	private DataSet<Tuple4<Integer, Double, Double, String>> readLineitem() {
//		final CsvReader source = getCSVReader(PathConfig.LINEITEM);
//		return source.fieldDelimiter("|").includeFields("1000011010000000").types(Integer.class, Double.class,
//				Double.class, String.class);
//	}
//
//	private DataSet<Tuple2<Integer, String>> readNation() {
//		final CsvReader source = getCSVReader(PathConfig.NATION);
//		return source.fieldDelimiter("|").includeFields("1100").types(Integer.class, String.class);
//	}
//
//	private DataSet<Tuple3<Integer, Integer, String>> readOrders() {
//		final CsvReader source = getCSVReader(PathConfig.ORDERS);
//		return source.fieldDelimiter("|").includeFields("110010000").types(Integer.class, Integer.class, String.class);
//	}
//
//	private DataSet<Tuple7<Integer, String, String, Integer, String, Double, String>> readCustomer() {
//		final CsvReader source = getCSVReader(PathConfig.CUSTOMER);
//		return source.fieldDelimiter("|").includeFields("11111101").types(Integer.class, String.class, String.class,
//				Integer.class, String.class, Double.class, String.class);
//	}
//
//	private String getRandomDate() {
//		int year = Utils.getRandomInt(1993, 1995);
//		int month = Utils.getRandomInt(1, 12);
//		if (month == 1 && year == 1993) {
//			month = Utils.getRandomInt(2, 12);
//		}
//		String monthString;
//		if (month < 10) {
//			monthString = "-0" + month;
//		} else {
//			monthString = "-" + month;
//		}
//		return year + monthString + "-01";
//	}
//
//}
