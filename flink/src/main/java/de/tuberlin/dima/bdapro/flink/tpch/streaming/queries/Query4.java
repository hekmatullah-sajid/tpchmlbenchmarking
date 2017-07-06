//package de.tuberlin.dima.bdapro.flink.tpch.streaming.queries;
//
//import java.time.LocalDate;
//import java.time.format.DateTimeFormatter;
//import java.util.List;
//import java.util.Random;
//
//import org.apache.flink.api.common.functions.FilterFunction;
//import org.apache.flink.api.common.functions.GroupReduceFunction;
//import org.apache.flink.api.common.functions.JoinFunction;
//import org.apache.flink.api.common.functions.MapFunction;
//import org.apache.flink.api.common.operators.Order;
//import org.apache.flink.api.java.DataSet;
//import org.apache.flink.api.java.ExecutionEnvironment;
//import org.apache.flink.api.java.io.CsvReader;
//import org.apache.flink.api.java.tuple.Tuple1;
//import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.api.java.tuple.Tuple3;
//import org.apache.flink.util.Collector;
//
//import de.tuberlin.dima.bdapro.flink.tpch.PathConfig;
//
///**
// * Created by seema on 24/05/2017.
// */
//public class Query4 extends Query {
//
//	LocalDate date = getRandomDate();
//	private static DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
//	public Query4(final ExecutionEnvironment env, final String sf)
//	{
//		super(env,sf);
//	}
//	@Override
//	public List<Tuple2<String,Long>> execute() {
//		DataSet<Tuple3<Long,String,String>> orders = readOrders();
//		DataSet<Tuple3<Long,String,String>> lineItems = readLineItems();
//		orders = orders.filter(filterOrders(this.date));
//		DataSet<Tuple1<Long>> l_orderkey =  lineItems.filter(filterLineItems())
//				.map(new RetrieveOrderKey())
//				.distinct();
//		DataSet<Tuple2<String,Long >> result = orders.join(l_orderkey)
//				.where(0).equalTo(0)
//				.with(new JoinFunction<Tuple3<Long, String, String>, Tuple1<Long>, Tuple1<String>>(){
//					@Override
//					public Tuple1<String> join(final Tuple3<Long, String, String> orders, final Tuple1<Long> lineitems) {
//						return new Tuple1<String>(orders.f2);
//					}
//				}).groupBy(0).reduceGroup(new CountItemsInGroup()).sortPartition(0, Order.ASCENDING);
//
//		try {
//			return result.collect();
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//		return null;
//	}
//
//	private FilterFunction<Tuple3<Long, String, String>> filterLineItems() {
//		return (FilterFunction<Tuple3<Long, String, String>>) value -> {
//			LocalDate commitdate = LocalDate.parse(value.f1, dateTimeFormatter);
//			LocalDate receiptdate = LocalDate.parse(value.f2, dateTimeFormatter);
//			return (commitdate.isBefore(receiptdate));
//		};
//	}
//
//	private FilterFunction<Tuple3<Long, String, String>> filterOrders(final LocalDate randomDate) {
//		return (FilterFunction<Tuple3<Long, String, String>>) value -> {
//			LocalDate date = LocalDate.parse(value.f1, dateTimeFormatter);
//			return (date.isAfter(randomDate) || date.isEqual(randomDate)) && (date.isBefore(randomDate.plusMonths(3)));
//		};
//	}
//	private LocalDate getRandomDate()
//	{
//		Random rand = new Random();
//		int year = rand.nextInt((5) + 1993);
//		int month;
//		if(year < 1997) {
//			month = rand.nextInt(13) + 1;//) is inclusive in nextInt, so add 1
//		} else {
//			month = rand.nextInt(11) + 1;
//		}
//		return LocalDate.of(year, month, 1);
//	}
//
//	public void setDate(final LocalDate date)
//	{
//		this.date = date;
//	}
//
//	//read Orders OrderKey, OrderDate, OrderPriority
//	private DataSet<Tuple3<Long,String,String>> readOrders(){
//		CsvReader source = getCSVReader(PathConfig.ORDERS);
//		return source.fieldDelimiter("|").includeFields("100011000").types(Long.class, String.class, String.class);
//	}
//
//	//read Lineitems: OrderKey,commit date, receipt date
//	private DataSet<Tuple3<Long,String,String>> readLineItems(){
//		CsvReader source = getCSVReader(PathConfig.LINEITEM);
//		return source.fieldDelimiter("|").includeFields("1000000000011000").types(Long.class, String.class, String.class);
//	}
//
//	private class CountItemsInGroup implements GroupReduceFunction<Tuple1<String>, Tuple2<String,Long>> {
//		@Override
//		public void reduce(final Iterable<Tuple1<String>> iterable, final Collector<Tuple2<String, Long>> collector) throws Exception {
//			Long count = 0L;
//			String key = null;
//			for (Tuple1<String> t : iterable) {
//				key = t.f0;
//				count++;
//			}
//			collector.collect(new Tuple2<>(key, count));
//		}
//	}
//
//	private class RetrieveOrderKey implements MapFunction<Tuple3<Long, String, String>, Tuple1<Long>> {
//		@Override
//		public Tuple1<Long> map(final Tuple3<Long, String, String> record) throws Exception {
//			return new Tuple1<Long>(record.f0);
//		}
//	}
//}
