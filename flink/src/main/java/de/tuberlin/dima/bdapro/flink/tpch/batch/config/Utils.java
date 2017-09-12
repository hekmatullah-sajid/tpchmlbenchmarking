package de.tuberlin.dima.bdapro.flink.tpch.batch.config;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.apache.flink.api.java.tuple.Tuple;

/**
 * The Utils class is used to define the functions used in TPC-H Benchmark
 * queries for getting random values for the substitution parameters that must
 * be generated and used to build the executable query text.
 * 
 * @author Hekmatullah Sajid and Seema Narasimha Swamy
 *
 */
public class Utils {

	/**
	 * Enum for listing Nations and related Regions, random nations and/or
	 * regions are selected from this enum based on the query requirements.
	 */
	public static enum Nation{
		ALGERIA("ALGERIA", "AFRICA"), 
		ARGENTINA("ARGENTINA", "AMERICA"), 
		BRAZIL("BRAZIL", "AMERICA"),
		CANADA("CANADA", "AMERICA"),
		EGYPT("EGYPT", "MIDDLE EAST"),
		ETHIOPIA("ETHIOPIA", "AFRICA"),
		FRANCE("FRANCE", "EUROPE"),
		GERMANY("GERMANY", "EUROPE"),
		INDIA("INDIA", "ASIA"),
		INDONESIA("INDONESIA", "ASIA"),
		IRAN("IRAN", "MIDDLE EAST"),
		IRAQ("IRAQ", "MIDDLE EAST"),
		JAPAN("JAPAN", "ASIA"),
		JORDAN("JORDAN", "MIDDLE EAST"),
		KENYA("KENYA", "AFRICA"),
		MOROCCO("MOROCCO", "AFRICA"),
		MOZAMBIQUE("MOZAMBIQUE", "AFRICA"),
		PERU("PERU", "AMERICA"),
		CHINA("CHINA", "ASIA"),
		ROMANIA("ROMANIA", "EUROPE"),
		SAUDI_ARABIA("SAUDI ARABIA", "MIDDLE EAST"),
		VIETNAM("VIETNAM", "ASIA"),
		RUSSIA("RUSSIA", "EUROPE"),
		UNITED_KINGDOM("UNITED KINGDOM", "EUROPE"),
		UNITED_STATES("UNITED STATES", "AMERICA");
		private String name;

		private String region;

		private Nation(final String value, final String region) {
			this.name = value;
			this.region = region;
		}

		public String getName() {
			return this.name;
		}

		public String getRegion() {
			return this.region;
		}

		public static Nation getRandomNationAndRegion() {
			int random = new Random().nextInt(values().length);
			return values()[random];
		}

		public static String getRandomNation() {
			return getRandomNationAndRegion().getName();
		}

		public static String getRandomRegion() {
			return getRandomNationAndRegion().getRegion();
		}
	}

	/*
	 * Each query requires one or more substitution parameters that must be
	 * generated and used to build the executable query text. Different lists
	 * are defined to be used as a source for generating the substitution
	 * parameters. These random values are used in SQL conditions for defining
	 * the intended output.
	 */
	private static final List<String> SEGMENTS = new ArrayList<>(
			Arrays.asList("AUTOMOBILE", "BUILDING", "FURNITURE", "MACHINERY", "HOUSEHOLD"));

	private static final List<String> TYPE_SYL3 = new ArrayList<>(
			Arrays.asList("TIN", "NICKEL", "BRASS", "STEEL", "COPPER"));

	private static final List<String> TYPE_SYL2 = new ArrayList<>(
			Arrays.asList("ANODIZED", "BURNISHED", "PLATED", "POLISHED", "BRUSHED"));

	private static final List<String> TYPE_SYL1 = new ArrayList<>(
			Arrays.asList("STANDARD", "SMALL", "MEDIUM", "LARGE", "ECONOMY", "PROMO"));

	private static final List<String> WORD_1 = new ArrayList<>(
			Arrays.asList("special", "pending", "unusual", "express"));

	private static final List<String> WORD_2 = new ArrayList<>(
			Arrays.asList("packages", "requests", "accounts", "deposits"));

	private static final List<String> SHIPMODES = new ArrayList<>(
			Arrays.asList("REG AIR", "AIR", "RAIL", "SHIP", "TRUCK", "MAIL", "FOB"));

	private static final List<String> CONTAINERS_SYL1 = new ArrayList<>(
			Arrays.asList("SM", "LG", "MED", "JUMBO", "WRAP"));

	private static final List<String> CONTAINERS_SYL2 = new ArrayList<>(
			Arrays.asList("CASE", "BOX", "BAG", "JAR", "PKG", "PACK", "CAN", "DRUM"));

	private static final List<String> COLORS = new ArrayList<>(Arrays.asList("almond", "antique", "aquamarine", "azure",
			"beige", "bisque", "black", "blanched", "blue", "blush", "brown", "burlywood", "burnished", "chartreuse",
			"chiffon", "chocolate", "coral", "cornflower", "cornsilk", "cream", "cyan", "dark", "deep", "dim", "dodger",
			"drab", "firebrick", "floral", "forest", "frosted", "gainsboro", "ghost", "goldenrod", "green", "grey",
			"honeydew", "hot", "indian", "ivory", "khaki", "lace", "lavender", "lawn", "lemon", "light", "lime",
			"linen", "magenta", "maroon", "medium", "metallic", "midnight", "mint", "misty", "moccasin", "navajo",
			"navy", "olive", "orange", "orchid", "pale", "papaya", "peach", "peru", "pink", "plum", "powder", "puff",
			"purple", "red", "rose", "rosy", "royal", "saddle", "salmon", "sandy", "seashell", "sienna", "sky", "slate",
			"smoke", "snow", "spring", "steel", "tan", "thistle", "tomato", "turquoise", "violet", "wheat", "white",
			"yellow"));

	/**
	 * Selects a random values from the list passed as parameter.
	 * 
	 * @param list
	 *            a list from defined lists is passed as a parameter.
	 * @return random value from the given list.
	 */
	private static String getRandomElementFromList(final List<String> list) {
		Random rand = new Random();
		return list.get(rand.nextInt(list.size()));
	}

	/*
	 * Methods following the comment are returning a random value from one of
	 * the list specified above.
	 * 
	 */

	/**
	 * The function is used to get random Shipmode substitution parameter from
	 * the list SHIPMODES.
	 * 
	 * @return a randomly selected ship mode.
	 */
	public static String getRandomShipmode() {
		return getRandomElementFromList(SHIPMODES);
	}

	/**
	 * The function is used to get random Segment substitution parameter from
	 * the list SEGMENTS.
	 * 
	 * @return a randomly selected segment.
	 */
	public static String getRandomSegment() {
		return getRandomElementFromList(SEGMENTS);
	}

	/**
	 * The function is used to get random Type substitution parameter from the
	 * lists TYPE_SYL1, TYPE_SYL2 and TYPE_SYL1 separated by space.
	 * 
	 * @return a randomly selected type.
	 */
	public static String getRandomType() {
		return getRandomElementFromList(TYPE_SYL1) + " " + getRandomElementFromList(TYPE_SYL2) + " "
				+ getRandomElementFromList(TYPE_SYL3);
	}

	/**
	 * The function is used to get random type substitution parameter from the
	 * lists TYPE_SYL1 and TYPE_SYL2 separated by space.
	 * 
	 * @return a randomly selected type.
	 */
	public static String getRandomType2() {
		return getRandomElementFromList(TYPE_SYL1) + " " + getRandomElementFromList(TYPE_SYL2);
	}

	/**
	 * The function is used to get random type substitution parameter from the
	 * list TYPE_SYL1.
	 * 
	 * @return a randomly selected type.
	 */
	public static String getRandomTypeSyl1() {
		return getRandomElementFromList(TYPE_SYL1);
	}

	/**
	 * The function is used to get random type substitution parameter from the
	 * list TYPE_SYL2.
	 * 
	 * @return a randomly selected type.
	 */
	public static String getRandomTypeSyl2() {
		return getRandomElementFromList(TYPE_SYL2);
	}

	/**
	 * The function is used to get random type substitution parameter from the
	 * list TYPE_SYL3.
	 * 
	 * @return a randomly selected type.
	 */
	public static String getRandomTypeSyl3() {
		return getRandomElementFromList(TYPE_SYL3);
	}

	/**
	 * The function is used to get random word substitution parameter from the
	 * list WORD_1.
	 * 
	 * @return a randomly selected word.
	 */
	public static String getRandomWord1() {
		return getRandomElementFromList(WORD_1);
	}

	/**
	 * The function is used to get random word substitution parameter from the
	 * list WORD_2.
	 * 
	 * @return a randomly selected word.
	 */
	public static String getRandomWord2() {
		return getRandomElementFromList(WORD_2);
	}

	/**
	 * The function is used to get random segment substitution Container from
	 * the lists CONTAINERS_SYL1 and CONTAINERS_SYL2 separated by space.
	 * 
	 * @return a randomly selected segment.
	 */
	public static String getRandomContainer() {
		return getRandomElementFromList(CONTAINERS_SYL1) + " " + getRandomElementFromList(CONTAINERS_SYL2);
	}

	/**
	 * The function is used to get random brand substitution parameter. The
	 * random brand is selected as "Brand#MN" where M and N are two single
	 * character strings representing two numbers randomly and independently
	 * selected within [1 .. 5];
	 * 
	 * @return a randomly selected segment.
	 */
	public static String getRandomBrand() {
		Random rand = new Random();
		String firstDigit = String.valueOf(rand.nextInt(6) + 1);
		String secondDigit = String.valueOf(rand.nextInt(6) + 1);
		return "Brand#" + firstDigit + secondDigit;
	}

	/**
	 * The function is used to get random color substitution parameter from the
	 * list COLORS.
	 * 
	 * @return a randomly selected color.
	 */
	public static String getRandomColor() {
		return getRandomElementFromList(COLORS);
	}

	/**
	 * Check all double fields in input tuple and keep only two decimal place.
	 * This method is used to drop more than two decimal values in the result of
	 * query. The example output given for JUnit tests have two decimal values,
	 * without this the Unit tests fails.
	 * 
	 * @param tuple
	 * @return double variables with two decimal values
	 */
	@SuppressWarnings("unchecked")
	public static <T extends Tuple> T keepOnlyTwoDecimals(final Tuple tuple) {
		final int size = tuple.getArity();
		for (int i = 0; i < size; i++) {
			if (tuple.getField(i) instanceof Double) {
				tuple.setField(convertToTwoDecimal((double) tuple.getField(i)), i);
			}
		}
		return (T) tuple;
	}

	public static double convertToTwoDecimal(final double value) {
		return Math.round(value * 100.0) / 100.0;
	}

	/**
	 * Get a random integer between two values (both are inclusive).
	 * 
	 * @param upperLimit
	 * @param bottomLimit
	 * @return random integer
	 */
	public static int getRandomInt(final int bottomLimit, final int upperLimit) {
		final Random rand = new Random();
		return bottomLimit + rand.nextInt((upperLimit - bottomLimit) + 1);
	}

	/**
	 * Get a random double between two values (both are inclusive).
	 * 
	 * @param upperLimit
	 * @param bottomLimit
	 * @return random double
	 */
	public static double getRandomDouble(final double bottomLimit, final double upperLimit) {
		final Random rand = new Random();
		return bottomLimit + (upperLimit - bottomLimit) * rand.nextDouble();
	}

}
