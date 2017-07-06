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
//import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.api.java.tuple.Tuple4;
//import org.apache.flink.util.Collector;
//
//import de.tuberlin.dima.bdapro.flink.tpch.PathConfig;
//import de.tuberlin.dima.bdapro.flink.tpch.Utils;
//
///**
// * Created by seema on 28/05/2017.
// */
//public class Query16 extends Query {
//
//	String brand = Utils.getRandomBrand();
//	String type = Utils.getRandomType2();
//	public Query16(final ExecutionEnvironment env, final String sf) {
//		super(env, sf);
//	}
//
//	@Override
//	public List<Tuple4<String,String,Integer, Long>> execute() {
//		DataSet<Tuple4<Long, String, String, Integer>> part = readPart();
//		DataSet<Tuple2<Long, Long>> partSupp = readPartSupp();
//		DataSet<Tuple2<Long, String>> supplier = readSupplier();
//
//		DataSet<Tuple4<Long, String, String, Integer>> filteredPart = part.filter(filterPart(brand, type));
//
//		DataSet<Tuple2<Long, Long>> innerQuery = supplier.filter(value -> {return !value.f1.matches(".*Customer.*Complaints.*");})
//				.join(partSupp).where(0).equalTo(1).projectSecond(0,1);
//
//
//		DataSet<Tuple4<String,String,Integer,Long>> result = filteredPart.join(innerQuery).where(0).equalTo(0)
//				.with(new JoinFunction<Tuple4<Long, String, String, Integer>, Tuple2<Long, Long>, Tuple4<String,String,Integer,Long>>() {
//					@Override
//					public Tuple4<String, String, Integer, Long> join(final Tuple4<Long, String, String, Integer> part, final Tuple2<Long, Long> innerquery) {
//						return new Tuple4<String, String, Integer, Long>(part.f1, part.f2, part.f3, innerquery.f1);
//					}
//				})
//				.groupBy(0,1,2).reduceGroup(new CountItemsInGroup())
//				.sortPartition(3, Order.ASCENDING)
//				.sortPartition(0, Order.ASCENDING)
//				.sortPartition(1, Order.ASCENDING)
//				.sortPartition(2, Order.ASCENDING);
//		try {
//			result.print();
//			return result.collect();
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//		return null;
//	}
//
//	public void setBrand(final String brand)
//	{
//		this.brand = brand;
//	}
//
//	public void setType(final String type)
//	{
//		this.type = type;
//	}
//
//	private FilterFunction<Tuple4<Long, String, String, Integer>> filterPart(final String brand, final String type) {
//		return (FilterFunction<Tuple4<Long, String, String, Integer>>) value -> {
//			return (!value.f1.equals(brand)) && (!value.f2.startsWith(type)) && (value.f3.toString().matches("49|14|23|45|19|3|36|9"));
//		};
//	}
//	//read Part: PartKey,Brand, Type,Size
//	private DataSet<Tuple4<Long,String,String,Integer>> readPart(){
//		CsvReader source = getCSVReader(PathConfig.PART);
//		return source.fieldDelimiter("|").includeFields("100111000").types(Long.class, String.class, String.class, Integer.class);
//	}
//
//	//read PartSupp: PartKey,SuppKey
//	private DataSet<Tuple2<Long,Long>> readPartSupp(){
//		CsvReader source = getCSVReader(PathConfig.PARTSUPP);
//		return source.fieldDelimiter("|").includeFields("11000").types(Long.class, Long.class);
//	}
//	//suppKey,comment
//	private DataSet<Tuple2<Long,String>> readSupplier(){
//		CsvReader source = getCSVReader(PathConfig.SUPPLIER);
//		return source.fieldDelimiter("|").includeFields("1000001").types(Long.class, String.class);
//	}
//
//	private class CountItemsInGroup implements GroupReduceFunction<Tuple4<String,String,Integer,Long>, Tuple4<String,String,Integer,Long>> {
//		@Override
//		public void reduce(final Iterable<Tuple4<String, String, Integer, Long>> iterable, final Collector<Tuple4<String, String, Integer, Long>> collector) throws Exception {
//			Long count = 0L;
//			Tuple4<String,String,Integer,Long> key = null;
//			for (Tuple4<String,String,Integer,Long> t : iterable) {
//				key = t;
//				count++;
//			}
//			collector.collect(new Tuple4<String,String,Integer,Long>(key.f0, key.f1, key.f2, count));
//
//		}
//	}
//}
