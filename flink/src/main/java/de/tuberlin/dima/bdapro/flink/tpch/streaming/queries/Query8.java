//package de.tuberlin.dima.bdapro.flink.tpch.streaming.queries;
//
//import java.time.LocalDate;
//import java.util.List;
//
//import org.apache.flink.api.common.functions.FilterFunction;
//import org.apache.flink.api.common.functions.GroupReduceFunction;
//import org.apache.flink.api.common.functions.MapFunction;
//import org.apache.flink.api.common.operators.Order;
//import org.apache.flink.api.java.DataSet;
//import org.apache.flink.api.java.ExecutionEnvironment;
//import org.apache.flink.api.java.io.CsvReader;
//import org.apache.flink.api.java.tuple.Tuple1;
//import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.api.java.tuple.Tuple3;
//import org.apache.flink.api.java.tuple.Tuple4;
//import org.apache.flink.api.java.tuple.Tuple5;
//import org.apache.flink.util.Collector;
//
//import de.tuberlin.dima.bdapro.flink.tpch.PathConfig;
//import de.tuberlin.dima.bdapro.flink.tpch.Utils;
//import de.tuberlin.dima.bdapro.flink.tpch.Utils.Nation;
//
//public class Query8 extends Query {
//
//	public Query8(final ExecutionEnvironment env, final String sf) {
//		super(env, sf);
//	}
//
//	@Override
//	public List<Tuple2<Integer, Double>> execute() {
//		Nation nation = Nation.getRandomNationAndRegion();
//		return execute(nation.getName(), nation.getRegion(), Utils.getRandomType());
//	}
//
//	public List<Tuple2<Integer, Double>> execute(final String nation, final String region, final String type) {
//		// partkey, type
//		DataSet<Tuple2<Integer, String>> part = readPart().filter(new FilterFunction<Tuple2<Integer, String>>() {
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public boolean filter(final Tuple2<Integer, String> value) throws Exception {
//				return value.f1.equals(type);
//			}
//		});
//
//		// suppkey, nationkey
//		DataSet<Tuple2<Integer, Integer>> supplier = readSupplier();
//
//		// orderkey, partkey, suppkey, volume
//		DataSet<Tuple4<Integer, Integer, Integer, Double>> lineitem = readLineitem().map(
//				new MapFunction<Tuple5<Integer, Integer, Integer, Double, Double>, Tuple4<Integer, Integer, Integer, Double>>() {
//					private static final long serialVersionUID = 1L;
//
//					@Override
//					public Tuple4<Integer, Integer, Integer, Double> map(
//							final Tuple5<Integer, Integer, Integer, Double, Double> value) throws Exception {
//						return new Tuple4<Integer, Integer, Integer, Double>(value.f0, value.f1, value.f2,
//								value.f3 * (1 - value.f4));
//					}
//				});
//
//		// orderkey, custkey, orderyear
//		DataSet<Tuple3<Integer, Integer, Integer>> orders = readOrders()
//				.filter(new FilterFunction<Tuple3<Integer, Integer, String>>() {
//					private static final long serialVersionUID = 1L;
//
//					@Override
//					public boolean filter(final Tuple3<Integer, Integer, String> value) throws Exception {
//						LocalDate val = LocalDate.parse(value.f2);
//						return (val.equals(LocalDate.of(1995, 1, 1)) || val.isAfter(LocalDate.of(1995, 1, 1)))
//								&& (val.equals(LocalDate.of(1996, 12, 31)) || val.isBefore(LocalDate.of(1996, 12, 31)));
//					}
//				}).map(new MapFunction<Tuple3<Integer, Integer, String>, Tuple3<Integer, Integer, Integer>>() {
//					private static final long serialVersionUID = 1L;
//
//					@Override
//					public Tuple3<Integer, Integer, Integer> map(final Tuple3<Integer, Integer, String> value)
//							throws Exception {
//						return new Tuple3<Integer, Integer, Integer>(value.f0, value.f1,
//								LocalDate.parse(value.f2).getYear());
//					}
//				});
//
//		// custkey, nationkey
//		DataSet<Tuple2<Integer, Integer>> customer = readCustomer();
//
//		// nationkey, name, regionkey
//		DataSet<Tuple3<Integer, String, Integer>> nations = readNation();
//
//		// regionkey, name
//		DataSet<Tuple2<Integer, String>> regions = readRegion().filter(new FilterFunction<Tuple2<Integer, String>>() {
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public boolean filter(final Tuple2<Integer, String> value) throws Exception {
//				return value.f1.equals(region);
//			}
//		});
//
//		// join supplier with nation - suppkey, nationName
//		DataSet<Tuple2<Integer, String>> suppNation = supplier.joinWithTiny(nations).where(1).equalTo(0).projectFirst(0)
//				.projectSecond(1);
//
//		// join nation with region - nationkey
//		DataSet<Tuple1<Integer>> regionNation = nations.join(regions).where(2).equalTo(0).projectFirst(0);
//
//		// join customer with nation - custkey
//		DataSet<Tuple1<Integer>> custNation = customer.joinWithTiny(regionNation).where(1).equalTo(0).projectFirst(0);
//
//		// join orders with customers - orderkey, orderyear
//		DataSet<Tuple2<Integer, Integer>> orderCust = orders.join(custNation).where(1).equalTo(0).projectFirst(0, 2);
//
//		// join part with lineitem - orderkey, suppkey, volume
//		DataSet<Tuple3<Integer, Integer, Double>> partLineitem = part.joinWithHuge(lineitem).where(0).equalTo(1)
//				.projectSecond(0, 2, 3);
//
//		// join lineitem with orders - suppkey, orderyear, volume
//		DataSet<Tuple3<Integer, Integer, Double>> lineitemOrders = partLineitem.joinWithTiny(orderCust).where(0)
//				.equalTo(0).projectFirst(1).projectSecond(1).projectFirst(2);
//
//		// join supplier with lineitem - orderyear, volume, name
//		DataSet<Tuple3<Integer, Double, String>> innerJoin = suppNation.joinWithHuge(lineitemOrders).where(0).equalTo(0)
//				.projectSecond(1, 2).projectFirst(1);
//
//		try {
//			return innerJoin.groupBy(0)
//					.reduceGroup(new GroupReduceFunction<Tuple3<Integer, Double, String>, Tuple2<Integer, Double>>() {
//						private static final long serialVersionUID = 1L;
//
//						@Override
//						public void reduce(final Iterable<Tuple3<Integer, Double, String>> values,
//								final Collector<Tuple2<Integer, Double>> out) throws Exception {
//							double sum1 = 0;
//							double sum2 = 0;
//							int year = 0;
//							for (Tuple3<Integer, Double, String> tuple : values) {
//								if (tuple.f2.equals(nation)) {
//									sum1 += tuple.f1;
//									year = tuple.f0;
//								}
//								sum2 += tuple.f1;
//							}
//							out.collect(new Tuple2<Integer, Double>(year, Utils.convertToTwoDecimal(sum1 / sum2)));
//
//						}
//					}).sortPartition(0, Order.ASCENDING).collect();
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//
//		return null;
//	}
//
//	private DataSet<Tuple5<Integer, Integer, Integer, Double, Double>> readLineitem() {
//		final CsvReader source = getCSVReader(PathConfig.LINEITEM);
//		return source.fieldDelimiter("|").includeFields("1110011000000000").types(Integer.class, Integer.class,
//				Integer.class, Double.class, Double.class);
//	}
//
//	private DataSet<Tuple3<Integer, String, Integer>> readNation() {
//		final CsvReader source = getCSVReader(PathConfig.NATION);
//		return source.fieldDelimiter("|").includeFields("1110").types(Integer.class, String.class, Integer.class);
//	}
//
//	private DataSet<Tuple2<Integer, String>> readRegion() {
//		final CsvReader source = getCSVReader(PathConfig.REGION);
//		return source.fieldDelimiter("|").includeFields("110").types(Integer.class, String.class);
//	}
//
//	private DataSet<Tuple3<Integer, Integer, String>> readOrders() {
//		final CsvReader source = getCSVReader(PathConfig.ORDERS);
//		return source.fieldDelimiter("|").includeFields("110010000").types(Integer.class, Integer.class, String.class);
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
//	private DataSet<Tuple2<Integer, String>> readPart() {
//		final CsvReader source = getCSVReader(PathConfig.PART);
//		return source.fieldDelimiter("|").includeFields("100010000").types(Integer.class, String.class);
//	}
//
//}
