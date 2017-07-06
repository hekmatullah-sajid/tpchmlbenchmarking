//package de.tuberlin.dima.bdapro.flink.tpch.streaming.queries;
//
//import java.time.LocalDate;
//import java.util.List;
//
//import org.apache.flink.api.common.functions.FilterFunction;
//import org.apache.flink.api.common.functions.MapFunction;
//import org.apache.flink.api.java.DataSet;
//import org.apache.flink.api.java.ExecutionEnvironment;
//import org.apache.flink.api.java.aggregation.Aggregations;
//import org.apache.flink.api.java.io.CsvReader;
//import org.apache.flink.api.java.tuple.Tuple1;
//import org.apache.flink.api.java.tuple.Tuple4;
//
//import de.tuberlin.dima.bdapro.flink.tpch.PathConfig;
//import de.tuberlin.dima.bdapro.flink.tpch.Utils;
//
//public class Query6 extends Query {
//
//	public Query6(final ExecutionEnvironment env, final String sf) {
//		super(env, sf);
//	}
//
//	@Override
//	public List<Tuple1<Double>> execute() {
//		return execute(Utils.getRandomInt(1993, 1997) + "-01-01", Utils.getRandomDouble(0.02, 0.09), Utils.getRandomInt(24, 25));
//	}
//
//	public List<Tuple1<Double>> execute(final String date, final double discount, final int quantity) {
//
//		final LocalDate randDate = LocalDate.parse(date);
//		final double randDiscount = discount;
//		final int randQuantity = quantity;
//
//		try {
//			final DataSet<Tuple4<Double, Double, Double, String>> lineitem = readLineitem();
//			final List<Tuple1<Double>> out = lineitem
//					.filter(new FilterFunction<Tuple4<Double, Double, Double, String>>() {
//						private static final long serialVersionUID = 1L;
//
//						@Override
//						public boolean filter(final Tuple4<Double, Double, Double, String> value) throws Exception {
//							double lowerBoundDiscount = Utils.convertToTwoDecimal(randDiscount - 0.01);
//							double upperBoundDiscount = Utils.convertToTwoDecimal(randDiscount + 0.01);
//							return (value.f2 >= lowerBoundDiscount && value.f2 <= upperBoundDiscount)
//									&& (value.f0 < randQuantity)
//									&& (LocalDate.parse(value.f3).isEqual(randDate)
//											|| LocalDate.parse(value.f3).isAfter(randDate))
//									&& (LocalDate.parse(value.f3).isBefore(randDate.plusYears(1)));
//						}
//					}).map(new MapFunction<Tuple4<Double, Double, Double, String>, Tuple1<Double>>() {
//						private static final long serialVersionUID = 1L;
//
//						@Override
//						public Tuple1<Double> map(final Tuple4<Double, Double, Double, String> value) throws Exception {
//							return new Tuple1<Double>(value.f1 * value.f2);
//						}
//					}).aggregate(Aggregations.SUM, 0).map(new MapFunction<Tuple1<Double>, Tuple1<Double>>() {
//						private static final long serialVersionUID = 1L;
//
//						@Override
//						public Tuple1<Double> map(final Tuple1<Double> value) throws Exception {
//							return Utils.keepOnlyTwoDecimals(value);
//						}
//					}).collect();
//			return out;
//		} catch (final Exception e) {
//			e.printStackTrace();
//		}
//		return null;
//	}
//
//	private DataSet<Tuple4<Double, Double, Double, String>> readLineitem() {
//		final CsvReader source = getCSVReader(PathConfig.LINEITEM);
//		return source.fieldDelimiter("|").includeFields("0000111000100000").types(Double.class, Double.class,
//				Double.class, String.class);
//	}
//}
