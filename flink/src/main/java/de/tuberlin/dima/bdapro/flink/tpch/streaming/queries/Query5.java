//package de.tuberlin.dima.bdapro.flink.tpch.streaming.queries;
//
//import java.time.LocalDate;
//import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.List;
//import java.util.Random;
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
//
//import de.tuberlin.dima.bdapro.flink.tpch.PathConfig;
//import de.tuberlin.dima.bdapro.flink.tpch.Utils;
//
//public class Query5 extends Query {
//	// Region list to select one randomly for query
//	private List<String> regionList = new ArrayList<>(Arrays.asList("AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"));
//	String region = getRandomItem(regionList);
//	LocalDate randDate = LocalDate.parse(Utils.getRandomInt(1993, 1997) + "-01-01");
//	public Query5(final ExecutionEnvironment env, final String sf) {
//		super(env, sf);
//		// TODO Auto-generated constructor stub
//	}
//
//	/*
//	 * (non-Javadoc)
//	 * @see de.tuberlin.dima.bdapro.tpch.queries.Query#execute()
//	 * 
//	 * the following fields are needed from each table 
//	 * 
//	 * Customer(c_custkey, c_nationkey)10010000 
//	 * orders(o_orderkey, o_custkey, o_orderdate)110010000
//	 * lineitem(l_orderkey, l_suppkey, l_extendedprice, l_discount)1010011000000000
//	 * supplier(s_suppkey, s_nationkey)1001000
//	 * nation(n_nationkey, n_name, n_regionkey) 1110
//	 * region(r_regionkey, r_name)110
//	 * 
//	 */
//
//	public List<Tuple2<String, Double>> execute(final String testRegion, final String testDate) {
//		region = testRegion;
//		randDate = LocalDate.parse(testDate);
//		return execute();
//	}
//	@Override
//	public List<Tuple2<String, Double>> execute() {
//		// TODO Auto-generated method stub
//
//		// Customer(c_custkey, c_nationkey)
//		final DataSet<Tuple2<Integer, Integer>> customers = readCustomers();
//		// Orders(o_orderkey, o_custkey, o_orderdate)
//		DataSet<Tuple3<Integer, Integer, String>> orders = readOrders();
//		// supplier(s_suppkey, s_nationkey)
//		final DataSet<Tuple2<Integer, Integer>> suppliers = readSuppliers();
//		// lineitem(l_orderkey, l_suppkey, l_extendedprice, l_discount)
//		final DataSet<Tuple4<Integer, Integer, Double, Double>> lineitems = readLineitem();
//		// nation(n_nationkey, n_name, n_regionkey) 
//		final DataSet<Tuple3<Integer, String, Integer>> nations = readNations();
//		// Region(r_regionkey, r_name)
//		DataSet<Tuple2<Integer, String>> regions = readRegions();
//		try{
//			// First filter the datasets for given random values
//			orders = orders.filter(filterOrders(randDate));
//			regions = regions.filter(filterRegions(region));
//
//
//			DataSet<Tuple3<String, Double, Double>> partialRes =
//					// the join result will be temptbl(c_nationkey, o_orderkey)
//					customers.joinWithHuge(orders).where(0).equalTo(1).projectFirst(1).projectSecond(0)
//					// the join result will be temptbl(c_nationkey, l_suppkey, l_extendedprice, l_discount)
//					.joinWithHuge(lineitems).where(1).equalTo(0).projectFirst(0).projectSecond(1,2,3)
//					// the join result will be temptbl(c_nationkey, l_extendedprice, l_discount)
//					.joinWithTiny(suppliers).where(0,1).equalTo(1,0).projectFirst(0,2,3)
//					// the join result will be temptbl(l_extendedprice, l_discount, n_name, n_regionkey)
//					.joinWithTiny(nations).where(0).equalTo(0).projectFirst(1,2).projectSecond(1,2)
//					// the join result will be temptbl(n_name, l_extendedprice, l_discount)
//					.joinWithTiny(regions).where(3).equalTo(0).projectFirst(2,0,1);
//
//			final List<Tuple2<String, Double>> out = partialRes
//					.map(new MapFunction<Tuple3<String, Double, Double>, Tuple2<String, Double>>() {
//						private static final long serialVersionUID = 1L;
//
//						@Override
//						public Tuple2<String, Double> map(final Tuple3<String, Double, Double> value) throws Exception {
//							return new Tuple2<String, Double>(value.f0, value.f1 * (1 - value.f2));
//						}
//					}).groupBy(0).aggregate(Aggregations.SUM, 1).map(new MapFunction<Tuple2<String, Double>, Tuple2<String, Double>>() {
//						private static final long serialVersionUID = 1L;
//
//						@Override
//						public Tuple2<String, Double> map(final Tuple2<String, Double> value) throws Exception {
//							return Utils.keepOnlyTwoDecimals(value);
//						}
//					})
//					.sortPartition(1, Order.DESCENDING)
//					.collect();
//
//			return out;
//		}
//		catch(Exception e){
//			e.printStackTrace();
//		}
//		return null;
//	}
//
//	//Read Customers 
//	private DataSet<Tuple2<Integer, Integer>> readCustomers() {
//		final CsvReader source = getCSVReader(PathConfig.CUSTOMER);
//		return source.fieldDelimiter("|").includeFields("10010000")
//				.types(Integer.class, Integer.class);
//	}
//
//	//Read Orders 
//	private DataSet<Tuple3<Integer, Integer, String>> readOrders() {
//		final CsvReader source = getCSVReader(PathConfig.ORDERS);
//		return source.fieldDelimiter("|").includeFields("110010000")
//				.types(Integer.class, Integer.class, String.class);
//	}
//
//	//Read lineitems 
//	private DataSet<Tuple4<Integer, Integer, Double, Double>> readLineitem() {
//		final CsvReader source = getCSVReader(PathConfig.LINEITEM);
//		return source.fieldDelimiter("|").includeFields("1010011000000000")
//				.types(Integer.class, Integer.class, Double.class, Double.class);
//	}
//
//	//Read Suppliers 
//	private DataSet<Tuple2<Integer, Integer>> readSuppliers() {
//		final CsvReader source = getCSVReader(PathConfig.SUPPLIER);
//		return source.fieldDelimiter("|").includeFields("1001000")
//				.types(Integer.class, Integer.class);
//	}
//
//	//Read Natios 
//	private DataSet<Tuple3<Integer, String, Integer>> readNations() {
//		final CsvReader source = getCSVReader(PathConfig.NATION);
//		return source.fieldDelimiter("|").includeFields("1110")
//				.types(Integer.class, String.class, Integer.class);
//	}
//
//	//Read Regions 
//	private DataSet<Tuple2<Integer, String>> readRegions() {
//		final CsvReader source = getCSVReader(PathConfig.REGION);
//		return source.fieldDelimiter("|").includeFields("110")
//				.types(Integer.class, String.class);
//	}
//
//	// get a random item from a list of strings.
//	private String getRandomItem(final List<String> list){
//		Random randomizer = new Random();
//		String random = list.get(randomizer.nextInt(list.size()));
//		return random;
//	}
//
//	//filter orders
//	private FilterFunction<Tuple3<Integer, Integer, String>> filterOrders(final LocalDate date) {
//		return (FilterFunction<Tuple3<Integer, Integer, String>>) value -> 
//		(LocalDate.parse(value.f2).isEqual(date)
//				|| LocalDate.parse(value.f2).isAfter(date))
//		&& (LocalDate.parse(value.f2).isBefore(date.plusYears(1)));
//	}
//
//	//filter region
//	private FilterFunction<Tuple2<Integer, String>> filterRegions(final String rgn) {
//		return (FilterFunction<Tuple2<Integer, String>>) r -> r.f1.equals(rgn);
//	}
//}
