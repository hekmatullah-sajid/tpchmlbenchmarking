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
//import org.apache.flink.api.java.tuple.Tuple5;
//
//import de.tuberlin.dima.bdapro.flink.tpch.PathConfig;
//import de.tuberlin.dima.bdapro.flink.tpch.Utils;
//import de.tuberlin.dima.bdapro.flink.tpch.Utils.Nation;
//
//public class Query7 extends Query {
//
//	public Query7(final ExecutionEnvironment env, final String sf) {
//		super(env, sf);
//	}
//
//	@Override
//	public List<Tuple4<String, String, Integer, Double>> execute() {
//		String[] randNations = getTwoRandomNations();
//		return execute(randNations[0], randNations[1]);
//	}
//
//	public List<Tuple4<String, String, Integer, Double>> execute(final String nation1, final String nation2) {
//
//		// suppkey, nationkey
//		DataSet<Tuple2<Integer, Integer>> supplier = readSupplier();
//
//		// orderkey, suppkey, extendedprice, discount, shipdate
//		DataSet<Tuple5<Integer, Integer, Double, Double, String>> lineitem = readLineitem()
//				.filter(new FilterFunction<Tuple5<Integer, Integer, Double, Double, String>>() {
//					private static final long serialVersionUID = 1L;
//
//					@Override
//					public boolean filter(final Tuple5<Integer, Integer, Double, Double, String> value)
//							throws Exception {
//						LocalDate currDate = LocalDate.parse(value.f4);
//						LocalDate lowerBound = LocalDate.of(1995, 01, 01);
//						LocalDate upperBound = LocalDate.of(1996, 12, 31);
//						return (currDate.equals(lowerBound) || currDate.isAfter(lowerBound))
//								&& (currDate.equals(upperBound) || currDate.isBefore(upperBound));
//					}
//				});
//
//		// orderkey, suppkey, volume, year
//		DataSet<Tuple4<Integer, Integer, Double, Integer>> filteredLineitem = lineitem.map(
//				new MapFunction<Tuple5<Integer, Integer, Double, Double, String>, Tuple4<Integer, Integer, Double, Integer>>() {
//					private static final long serialVersionUID = 1L;
//
//					@Override
//					public Tuple4<Integer, Integer, Double, Integer> map(
//							final Tuple5<Integer, Integer, Double, Double, String> value) throws Exception {
//						return new Tuple4<Integer, Integer, Double, Integer>(value.f0, value.f1,
//								value.f2 * (1 - value.f3), LocalDate.parse(value.f4).getYear());
//					}
//				});
//
//		// orderkey, custkey
//		DataSet<Tuple2<Integer, Integer>> orders = readOrders();
//
//		// custkey, nationkey
//		DataSet<Tuple2<Integer, Integer>> customer = readCustomer();
//
//		// nationkey, name
//		DataSet<Tuple2<Integer, String>> nations1 = readNation().filter(new FilterFunction<Tuple2<Integer, String>>() {
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public boolean filter(final Tuple2<Integer, String> value) throws Exception {
//				return value.f1.equals(nation1);
//			}
//		});
//		DataSet<Tuple2<Integer, String>> nations2 = readNation().filter(new FilterFunction<Tuple2<Integer, String>>() {
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public boolean filter(final Tuple2<Integer, String> value) throws Exception {
//				return value.f1.equals(nation2);
//			}
//		});
//
//		// join supplier with nation1 - suppkey, nationName
//		DataSet<Tuple2<Integer, String>> suppNations = supplier.joinWithTiny(nations1).where(1).equalTo(0)
//				.projectFirst(0).projectSecond(1);
//
//		// join costumer with nation2 - custkey, nationName
//		DataSet<Tuple2<Integer, String>> custNations = customer.joinWithTiny(nations2).where(1).equalTo(0)
//				.projectFirst(0).projectSecond(1);
//
//		// join customer with orders - custkey, nationName, orderkey
//		DataSet<Tuple3<Integer, String, Integer>> custOrders = custNations.joinWithHuge(orders).where(0).equalTo(1)
//				.projectFirst(0, 1).projectSecond(0);
//
//		// join orders with lineitem - nationName, suppkey, volume, year
//		DataSet<Tuple4<String, Integer, Double, Integer>> custWithLineitem = custOrders.joinWithHuge(filteredLineitem)
//				.where(2).equalTo(0).projectFirst(1).projectSecond(1, 2, 3);
//
//		// join lineitem with supplier - suppNation, custNation, year, volume
//		DataSet<Tuple4<String, String, Integer, Double>> innerJoin = custWithLineitem.join(suppNations).where(1)
//				.equalTo(0).projectSecond(1).projectFirst(0, 3, 2);
//
//		try {
//			List<Tuple4<String, String, Integer, Double>> out = innerJoin.groupBy(0, 1, 2)
//					.aggregate(Aggregations.SUM, 3)
//					// take only two decimal
//					.map(new MapFunction<Tuple4<String, String, Integer, Double>, Tuple4<String, String, Integer, Double>>() {
//						private static final long serialVersionUID = 1L;
//
//						@Override
//						public Tuple4<String, String, Integer, Double> map(
//								final Tuple4<String, String, Integer, Double> value) throws Exception {
//							return Utils.keepOnlyTwoDecimals(value);
//						}
//					}).sortPartition(0, Order.ASCENDING).sortPartition(1, Order.ASCENDING)
//					.sortPartition(2, Order.ASCENDING).collect();
//			return out;
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//
//		return null;
//	}
//
//	private DataSet<Tuple5<Integer, Integer, Double, Double, String>> readLineitem() {
//		final CsvReader source = getCSVReader(PathConfig.LINEITEM);
//		return source.fieldDelimiter("|").includeFields("1010011000100000").types(Integer.class, Integer.class,
//				Double.class, Double.class, String.class);
//	}
//
//	private DataSet<Tuple2<Integer, String>> readNation() {
//		final CsvReader source = getCSVReader(PathConfig.NATION);
//		return source.fieldDelimiter("|").includeFields("1100").types(Integer.class, String.class);
//	}
//
//	private DataSet<Tuple2<Integer, Integer>> readOrders() {
//		final CsvReader source = getCSVReader(PathConfig.ORDERS);
//		return source.fieldDelimiter("|").includeFields("110000000").types(Integer.class, Integer.class);
//	}
//
//	private DataSet<Tuple2<Integer, Integer>> readCustomer() {
//		final CsvReader source = getCSVReader(PathConfig.CUSTOMER);
//		return source.fieldDelimiter("|").includeFields("10010000").types(Integer.class, Integer.class);
//	}
//
//	private DataSet<Tuple2<Integer, Integer>> readSupplier() {
//		final CsvReader source = getCSVReader(PathConfig.SUPPLIER);
//		return source.fieldDelimiter("|").includeFields("1001000").types(Integer.class, Integer.class);
//	}
//
//	private String[] getTwoRandomNations() {
//		String nation1 = Nation.getRandomNation();
//		String nation2 = Nation.getRandomNation();
//		if (nation1.equals(nation2)) {
//			return getTwoRandomNations();
//		} else {
//			return new String[] { nation1, nation2 };
//		}
//	}
//}
