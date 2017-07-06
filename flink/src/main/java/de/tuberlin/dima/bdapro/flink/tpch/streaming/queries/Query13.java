//package de.tuberlin.dima.bdapro.flink.tpch.streaming.queries;
//
//import java.util.List;
//
//import org.apache.flink.api.common.functions.FilterFunction;
//import org.apache.flink.api.common.functions.GroupReduceFunction;
//import org.apache.flink.api.common.functions.JoinFunction;
//import org.apache.flink.api.common.operators.Order;
//import org.apache.flink.api.java.DataSet;
//import org.apache.flink.api.java.ExecutionEnvironment;
//import org.apache.flink.api.java.io.CsvReader;
//import org.apache.flink.api.java.tuple.Tuple;
//import org.apache.flink.api.java.tuple.Tuple1;
//import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.api.java.tuple.Tuple3;
//import org.apache.flink.util.Collector;
//
//import de.tuberlin.dima.bdapro.flink.tpch.PathConfig;
//
//public class Query13 extends Query {
//
//	public Query13(final ExecutionEnvironment env, final String sf) {
//		super(env, sf);
//	}
//
//	@Override
//	public List<? extends Tuple> execute() {
//		return execute("special", "requests");
//	}
//
//	public List<Tuple2<Integer, Integer>> execute(final String word1, final String word2) {
//
//		// custkey
//		DataSet<Tuple1<Integer>> customer = readCustomer();
//
//		// orderkey, custkey, comment
//		DataSet<Tuple3<Integer, Integer, String>> orders = readOrders()
//				.filter(new FilterFunction<Tuple3<Integer, Integer, String>>() {
//					private static final long serialVersionUID = 1L;
//
//					@Override
//					public boolean filter(final Tuple3<Integer, Integer, String> value) throws Exception {
//						return !value.f2.contains(word1) || !value.f2.contains(word2);
//					}
//				});
//
//		// join customer with orders - custkey, count(orderkey)
//		DataSet<Tuple2<Integer, Integer>> join = customer
//				.leftOuterJoin(orders).where(0).equalTo(1)
//				.with(new JoinFunction<Tuple1<Integer>, Tuple3<Integer, Integer, String>, Tuple2<Integer, Integer>>() {
//					private static final long serialVersionUID = 1L;
//
//					@Override
//					public Tuple2<Integer, Integer> join(final Tuple1<Integer> first,
//							final Tuple3<Integer, Integer, String> second) throws Exception {
//						return new Tuple2<Integer, Integer>(first.f0, second == null ? -1 : second.f0);
//					}
//				})
//				.groupBy(0)
//				.reduceGroup(new GroupReduceFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
//					private static final long serialVersionUID = 1L;
//
//					@Override
//					public void reduce(final Iterable<Tuple2<Integer, Integer>> values,
//							final Collector<Tuple2<Integer, Integer>> out) throws Exception {
//						int count = 0;
//						int custkey = 0;
//						for (Tuple2<Integer, Integer> tuple : values) {
//							if (tuple.f1 != -1) {
//								count++;
//							}
//							custkey = tuple.f0;
//						}
//						out.collect(new Tuple2<Integer, Integer>(custkey, count));
//					}
//				});
//
//		// do outer query
//		try {
//			return join.groupBy(1)
//					.reduceGroup(new GroupReduceFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
//						private static final long serialVersionUID = 1L;
//
//						@Override
//						public void reduce(final Iterable<Tuple2<Integer, Integer>> values,
//								final Collector<Tuple2<Integer, Integer>> out) throws Exception {
//							int count = 0;
//							int c_count = 0;
//							for (Tuple2<Integer, Integer> tuple : values) {
//								count++;
//								c_count = tuple.f1;
//							}
//							out.collect(new Tuple2<Integer, Integer>(c_count, count));
//
//						}
//					}).sortPartition(1, Order.DESCENDING).sortPartition(0, Order.DESCENDING).collect();
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//		return null;
//	}
//
//	private DataSet<Tuple3<Integer, Integer, String>> readOrders() {
//		final CsvReader source = getCSVReader(PathConfig.ORDERS);
//		return source.fieldDelimiter("|").includeFields("110000001").types(Integer.class, Integer.class, String.class);
//	}
//
//	private DataSet<Tuple1<Integer>> readCustomer() {
//		final CsvReader source = getCSVReader(PathConfig.CUSTOMER);
//		return source.fieldDelimiter("|").includeFields("10000000").types(Integer.class);
//	}
//
//}
