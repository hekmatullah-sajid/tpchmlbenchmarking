//package de.tuberlin.dima.bdapro.flink.tpch.streaming.queries;
//
//
//import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.List;
//import java.util.Random;
//import java.util.concurrent.ThreadLocalRandom;
//
//import org.apache.flink.api.common.functions.FilterFunction;
//import org.apache.flink.api.common.functions.MapFunction;
//import org.apache.flink.api.common.operators.Order;
//import org.apache.flink.api.java.DataSet;
//import org.apache.flink.api.java.ExecutionEnvironment;
//import org.apache.flink.api.java.io.CsvReader;
//import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.api.java.tuple.Tuple3;
//import org.apache.flink.api.java.tuple.Tuple5;
//import org.apache.flink.api.java.tuple.Tuple7;
//import org.apache.flink.api.java.tuple.Tuple8;
//import org.apache.flink.api.java.tuple.Tuple9;
//
//import de.tuberlin.dima.bdapro.flink.tpch.PathConfig;
//
////Minimum Cost Supplier Query (Q2) -- TPC-H
//
//public class Query2 extends Query {
//
//	//List of type and region, we are going to select one randomly from the lists for the query.
//	private List<String> typeList = new ArrayList<>(Arrays.asList("TIN", "NICKEL", "BRASS", "STEEL", "COPPER"));
//	private List<String> regionList = new ArrayList<>(Arrays.asList("AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"));
//
//	public Query2(final ExecutionEnvironment env, final String sf) {
//		super(env, sf);
//	}
//
//	// Variables to filter parrtTbl for the given random type and size
//	String type = getRandomItem(typeList);
//	int size = getRandomSize();
//	// Variables to Filter RegionTbl for the given random region
//	String region = getRandomItem(regionList);
//
//	public List<Tuple8<Double, String, String, Integer, String, String, String, String>> execute(final String pType, final int pSize, final String rRegion) {
//		type = pType;
//		size = pSize;
//		region = rRegion;
//		return execute();
//	}
//
//	@Override
//	public List<Tuple8<Double, String, String, Integer, String, String, String, String>> execute() {
//
//		//Read the tables and store it in datasets
//		DataSet<Tuple5<Integer, String, String, String, Integer>> PartTbl = readPart();
//		DataSet<Tuple7<Integer, String, String, Integer, String, Double, String>> SupplierTbl = readSupplier();
//		DataSet<Tuple3<Integer, Integer, Double>> PartSuppTbl = readPartSupp();
//		DataSet<Tuple3<Integer, String, Integer>> NationTbl = readNation();
//		DataSet<Tuple2<Integer, String>> RegionTbl = readRegion();
//
//		try {
//
//			PartTbl = PartTbl.filter(filterParts(type, size));
//			RegionTbl = RegionTbl.filter(filterRegions(region));
//
//			//Part(p_partkey(int), p_name(String), p_mfgr(String), p_type(String),  p_size(int))
//			//Partsupp(ps_partkey(int), ps_suppkey(int), ps_suplycost(Double))
//			//Result(p_partkey(int), p_mfgr(String), ps_suppkey(int), ps_suplycost(Double))
//			DataSet<Tuple9<Integer, String, Double, String, String, Integer, String, Double, String>> outterQuery = 
//					PartTbl.join(PartSuppTbl).where(0).equalTo(0).projectFirst(0,2).projectSecond(1,2)
//
//					//partsWithSupplyDetail(p_partkey(int), p_mfgr(String), ps_suppkey(int), ps_suplycost(Double))
//					//SUPPLIER(s_suppkey(int), s_name(String), s_address(String), s_nationkey(int), s_phone(String), s_acctbal(double), s_comment(String));
//					//Result(p_partkey, p_mfgr, ps_suplycost, s_name, s_address, s_nationkey, s_phone, s_acctbal, s_comment);
//					.join(SupplierTbl).where(2).equalTo(0).projectFirst(0,1,3).projectSecond(1,2,3,4,5,6)
//
//					// partsWithSupplierDetail(p_partkey, p_mfgr, ps_suplycost, s_name, s_address, s_nationkey, s_phone, s_acctbal, s_comment);
//					// Nation(n_nationkey(int), n_name(String), n_regionkey(int))
//					// Result(p_partkey, p_mfgr, ps_suplycost, s_name, s_address, s_phone, s_acctbal, s_comment, n_name, n_regionkey);
//
//					.join(NationTbl).where(5).equalTo(0).projectFirst(0,1,2,3,4,6,7,8).projectSecond(1,2)
//
//					// partSupplierNation(p_partkey, p_mfgr, ps_suplycost, s_name, s_address, s_phone, s_acctbal, s_comment, n_name, n_regionkey);
//					// Region(r_regionkey, r_name)
//					// Result(p_partkey, p_mfgr, ps_suplycost, s_name, s_address, s_phone, s_acctbal, s_comment, n_name)
//					.join(RegionTbl).where(9).equalTo(0).projectFirst(0,1,2,3,4,5,6,7,8);
//
//			//Find min cost
//
//			DataSet<Tuple2<Integer, Double>> innerQuery = 
//					// Result at this join tempTbl(ps_suplycost, ps_nationkey)
//					PartSuppTbl.join(SupplierTbl).where(1).equalTo(0).projectFirst(0,2).projectSecond(3)
//					// Result at this join tempTbl(ps_suplycost, n_regionkey)
//					.join(NationTbl).where(2).equalTo(0).projectFirst(0,1).projectSecond(2)
//					// Result at this join tempTbl(ps_suplycost)
//					.join(RegionTbl).where(2).equalTo(0).projectFirst(0,1);
//
//			DataSet<Tuple2<Integer, Double>> minCost = innerQuery.groupBy(0).minBy(1);
//
//			// FInal result (s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment)
//			DataSet<Tuple8<Double, String, String, Integer, String, String, String, String>>
//			finalResult =
//			outterQuery.join(minCost).where(2).equalTo(1)
//			.projectFirst(6,3,8,0,1,4,5,7);
//
//			// Generate the list to return
//
//			List<Tuple8<Double, String, String, Integer, String, String, String, String>> out = finalResult
//					.map(new MapFunction<Tuple8<Double, String, String, Integer, String, String, String, String>, 
//							Tuple8<Double, String, String, Integer, String, String, String, String>>() {
//						/**
//						 * 
//						 */
//						private static final long serialVersionUID = 1L;
//
//						@Override
//						public Tuple8<Double, String, String, Integer, String, String, String, String> map(final Tuple8<Double, String, String, Integer, String, String, String, String> value) throws Exception {
//							return value;
//						}
//					}).sortPartition(0, Order.DESCENDING).sortPartition(1, Order.ASCENDING)
//					.sortPartition(2, Order.ASCENDING).sortPartition(3, Order.ASCENDING)
//					.first(100)
//					.collect();
//			return out;
//
//		} 
//		catch (Exception e) {
//			e.printStackTrace();
//		}
//		return null;
//	}
//
//
//	/* 
//	 * The following columns are needed from part table 
//	 * Part(p_partkey(int), p_name(String), p_mfgr(String), p_type(String),  p_size(int))
//	 */
//
//	private DataSet<Tuple5<Integer, String, String, String, Integer>> readPart() {
//		CsvReader source =  getCSVReader(PathConfig.PART);
//		return source.fieldDelimiter("|").includeFields("111011000").types(Integer.class, String.class, String.class, String.class, Integer.class);
//	}
//
//
//	/* 
//	 * The following columns are needed from supplier table
//	 * SUPPLIER(s_suppkey(int), s_name(String), s_address(String), s_nationkey(int), s_phone(String), 
//	 * s_acctbal(double), s_comment(String)); 
//	 */
//
//	private DataSet<Tuple7<Integer, String, String, Integer, String, Double, String>> readSupplier() {
//		CsvReader source = getCSVReader(PathConfig.SUPPLIER);
//		return source.fieldDelimiter("|").includeFields("1111111").types(Integer.class, String.class, String.class, Integer.class, 
//				String.class, Double.class, String.class);
//	}
//
//	/* 
//	 * The following columns are needed from partsupp table
//	 * Partsupp(ps_partkey(int), ps_suppkey(int), ps_suplycost(Double))
//	 */
//
//	private DataSet<Tuple3<Integer, Integer, Double>> readPartSupp() {
//		CsvReader source = getCSVReader(PathConfig.PARTSUPP);
//		return source.fieldDelimiter("|").includeFields("11010").types(Integer.class, Integer.class, Double.class);
//	}
//
//	/* 
//	 * The following columns are needed from Nation table
//	 * Nation(n_nationkey(int), n_name(String), n_regionkey(int))
//	 */
//	private DataSet<Tuple3<Integer, String, Integer>> readNation() {
//		CsvReader source = getCSVReader(PathConfig.NATION);
//		return source.fieldDelimiter("|").includeFields("1110").types(Integer.class, String.class, Integer.class);
//	}
//
//
//	/* 
//	 * The following columns are needed from Region table
//	 * Region(r_regionkey(int), r_name(int))
//	 */
//	private DataSet<Tuple2<Integer, String>> readRegion() {
//		CsvReader source = getCSVReader(PathConfig.REGION);
//		return source.fieldDelimiter("|").includeFields("110").types(Integer.class, String.class);
//	}
//
//	// get a random item from a list of strings.
//	private String getRandomItem(final List<String> list){
//		Random randomizer = new Random();
//		String random = list.get(randomizer.nextInt(list.size()));
//		return random;
//	}
//
//	//get random integer for size
//	private int getRandomSize() {
//		return ThreadLocalRandom.current().nextInt(1, 50 + 1);
//	}
//
//	//filter part 
//	private FilterFunction<Tuple5<Integer, String, String, String, Integer>> filterParts(final String typ, final int sz) {
//		return (FilterFunction<Tuple5<Integer, String, String, String, Integer>>) partRecord -> 
//		partRecord.f3.endsWith(typ) && partRecord.f4.equals(sz);
//	}
//
//	//filter region
//	private FilterFunction<Tuple2<Integer, String>> filterRegions(final String rgn) {
//		return (FilterFunction<Tuple2<Integer, String>>) r -> r.f1.equals(rgn);
//	}
//}