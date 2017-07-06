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
//import org.apache.flink.api.java.tuple.Tuple;
//import org.apache.flink.api.java.tuple.Tuple1;
//import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.api.java.tuple.Tuple3;
//import org.apache.flink.api.java.tuple.Tuple4;
//import org.apache.flink.api.java.tuple.Tuple6;
//import org.apache.flink.api.java.tuple.Tuple7;
//
//import de.tuberlin.dima.bdapro.flink.tpch.PathConfig;
//
//public class Query9 extends Query {
//
//	public Query9(final ExecutionEnvironment env, final String sf) {
//		super(env, sf);
//	}
//
//	@Override
//	public List<? extends Tuple> execute() {
//		return execute("green");
//	}
//
//	public List<Tuple3<String, Integer, Double>> execute(final String color) {
//
//		// partkey
//		DataSet<Tuple1<Integer>> part = readPart().filter(new FilterFunction<Tuple2<Integer, String>>() {
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public boolean filter(final Tuple2<Integer, String> value) throws Exception {
//				return value.f1.toLowerCase().contains(color);
//			}
//		}).project(0);
//
//		// suppkey, nationkey
//		DataSet<Tuple2<Integer, Integer>> supplier = readSupplier();
//
//		// orderkey, partkey, suppkey, quantity, extendedprice, discount
//		DataSet<Tuple6<Integer, Integer, Integer, Double, Double, Double>> lineitem = readLineitem();
//
//		// partkey, suppkey, supplycost
//		DataSet<Tuple3<Integer, Integer, Double>> partsupp = readPartsupp();
//
//		// orderkey, year
//		DataSet<Tuple2<Integer, Integer>> orders = readOrders()
//				.map(new MapFunction<Tuple2<Integer, String>, Tuple2<Integer, Integer>>() {
//					private static final long serialVersionUID = 1L;
//
//					@Override
//					public Tuple2<Integer, Integer> map(final Tuple2<Integer, String> value) throws Exception {
//						return new Tuple2<Integer, Integer>(value.f0, LocalDate.parse(value.f1).getYear());
//					}
//				});
//
//		// nationkey, nationname
//		DataSet<Tuple2<Integer, String>> nation = readNation();
//
//		// join supplier with nation - suppkey, nation
//		DataSet<Tuple2<Integer, String>> supplierNation = supplier.joinWithTiny(nation).where(1).equalTo(0)
//				.projectFirst(0).projectSecond(1);
//
//		// join lineitem with partsupp
//		DataSet<Tuple7<Integer, Integer, Integer, Double, Double, Double, Double>> lineitemPartsupp = lineitem
//				.joinWithTiny(partsupp).where(1, 2).equalTo(0, 1).projectFirst(0, 1, 2, 3, 4, 5).projectSecond(2);
//
//		// calculate amount - orderkey, partkey, suppkey, amount
//		DataSet<Tuple4<Integer, Integer, Integer, Double>> amount = lineitemPartsupp
//				.map(new MapFunction<Tuple7<Integer,Integer,Integer,Double,Double,Double,Double>, Tuple4<Integer, Integer, Integer, Double>>() {
//					private static final long serialVersionUID = 1L;
//
//					@Override
//					public Tuple4<Integer, Integer, Integer, Double> map(
//							final Tuple7<Integer, Integer, Integer, Double, Double, Double, Double> value) throws Exception {
//						return new Tuple4<Integer, Integer, Integer, Double>(value.f0, value.f1, value.f2,
//								(value.f4 * (1 - value.f5)) - (value.f6 * value.f3));
//					}
//				});
//
//		// join with supplier - orderkey, partkey, amount, nation
//		DataSet<Tuple4<Integer, Integer, Double, String>> withSupplier = amount.joinWithTiny(supplierNation).where(2).equalTo(0).projectFirst(0,1,3).projectSecond(1);
//
//		// join with part - orderkey, amount, nation
//		DataSet<Tuple3<Integer, Double, String>> withPart = withSupplier.join(part).where(1).equalTo(0).projectFirst(0,2,3);
//
//		// join with orders - nation, year, amount
//		DataSet<Tuple3<String, Integer, Double>> innerJoin = withPart.join(orders).where(0).equalTo(0).projectFirst(2).projectSecond(1).projectFirst(1);
//
//		try {
//
//			innerJoin.groupBy(0, 1).aggregate(Aggregations.SUM, 2).sortPartition(0, Order.ASCENDING)
//			.sortPartition(1, Order.DESCENDING).print();
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//
//		return null;
//	}
//
//	private DataSet<Tuple6<Integer, Integer, Integer, Double, Double, Double>> readLineitem() {
//		final CsvReader source = getCSVReader(PathConfig.LINEITEM);
//		return source.fieldDelimiter("|").includeFields("1110111000000000").types(Integer.class, Integer.class,
//				Integer.class, Double.class, Double.class, Double.class);
//	}
//
//	private DataSet<Tuple2<Integer, String>> readNation() {
//		final CsvReader source = getCSVReader(PathConfig.NATION);
//		return source.fieldDelimiter("|").includeFields("1100").types(Integer.class, String.class);
//	}
//
//	private DataSet<Tuple2<Integer, String>> readOrders() {
//		final CsvReader source = getCSVReader(PathConfig.ORDERS);
//		return source.fieldDelimiter("|").includeFields("100010000").types(Integer.class, String.class);
//	}
//
//	private DataSet<Tuple3<Integer, Integer, Double>> readPartsupp() {
//		final CsvReader source = getCSVReader(PathConfig.PARTSUPP);
//		return source.fieldDelimiter("|").includeFields("11010").types(Integer.class, Integer.class, Double.class);
//	}
//
//	private DataSet<Tuple2<Integer, Integer>> readSupplier() {
//		final CsvReader source = getCSVReader(PathConfig.SUPPLIER);
//		return source.fieldDelimiter("|").includeFields("1001000").types(Integer.class, Integer.class);
//	}
//
//	private DataSet<Tuple2<Integer, String>> readPart() {
//		final CsvReader source = getCSVReader(PathConfig.PART);
//		return source.fieldDelimiter("|").includeFields("110000000").types(Integer.class, String.class);
//	}
//}
