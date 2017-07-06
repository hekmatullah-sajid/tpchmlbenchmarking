package de.tuberlin.dima.bdapro.spark.tpch;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class Utils {

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

		private Nation(final String value, final String region){
			this.name = value;
			this.region = region;
		}

		public String getName(){
			return this.name;
		}

		public String getRegion(){
			return this.region;
		}

		public static Nation getRandomNationAndRegion(){
			int random = new Random().nextInt(values().length);
			return values()[random];
		}

		public static String getRandomNation(){
			return getRandomNationAndRegion().getName();
		}

		public static String getRandomRegion(){
			return getRandomNationAndRegion().getRegion();
		}
	}

	private static final List<String> SEGMENTS = new ArrayList<>(Arrays.asList("AUTOMOBILE", "BUILDING", "FURNITURE", "MACHINERY","HOUSEHOLD"));

	private static final List<String> TYPE_SYL3 = new ArrayList<>(Arrays.asList("TIN", "NICKEL", "BRASS", "STEEL", "COPPER"));

	private static final List<String> TYPE_SYL2 = new ArrayList<>(Arrays.asList("ANODIZED", "BURNISHED", "PLATED", "POLISHED", "BRUSHED"));

	private static final List<String> TYPE_SYL1 = new ArrayList<>(Arrays.asList("STANDARD", "SMALL", "MEDIUM", "LARGE", "ECONOMY", "PROMO"));

	private static final List<String> WORD_1 = new ArrayList<>(Arrays.asList("special", "pending", "unusual", "express"));

	private static final List<String> WORD_2 = new ArrayList<>(Arrays.asList("packages", "requests", "accounts", "deposits"));

	private static final List<String> SHIPMODES = new ArrayList<>(Arrays.asList("REG AIR", "AIR", "RAIL", "SHIP", "TRUCK", "MAIL", "FOB"));

	private static String getRandomElementFromList(final List<String> list){
		Random rand = new Random();
		return list.get(rand.nextInt(list.size()));
	}
	
	public static String getRandomShipmode() {
		return getRandomElementFromList(SHIPMODES);
	}
	
	public static String getRandomSegment() {
		return getRandomElementFromList(SEGMENTS);
	}

	public static String getRandomType() {
		return getRandomElementFromList(TYPE_SYL1) + " " + getRandomElementFromList(TYPE_SYL2) + " " + getRandomElementFromList(TYPE_SYL3);
	}

	public static String getRandomType2() {
		return getRandomElementFromList(TYPE_SYL1) + " " + getRandomElementFromList(TYPE_SYL2);
	}

	public static String getRandomTypeSyl1() {
		return getRandomElementFromList(TYPE_SYL1);
	}

	public static String getRandomTypeSyl2() {
		return getRandomElementFromList(TYPE_SYL2);
	}

	public static String getRandomTypeSyl3() {
		return getRandomElementFromList(TYPE_SYL3);
	}

	public static String getRandomWord1() {
		return getRandomElementFromList(WORD_1);
	}

	public static String getRandomWord2() {
		return getRandomElementFromList(WORD_2);
	}

	private static final List<String> CONTAINERS_SYL1 = new ArrayList<>(Arrays.asList("SM", "LG", "MED", "JUMBO", "WRAP"));

	private static final List<String> CONTAINERS_SYL2 = new ArrayList<>(Arrays.asList("CASE", "BOX", "BAG", "JAR", "PKG", "PACK", "CAN", "DRUM"));

	public static String getRandomContainer() {
		return getRandomElementFromList(CONTAINERS_SYL1) + " " + getRandomElementFromList(CONTAINERS_SYL2);
	}

	public static String getRandomBrand() {
		Random rand = new Random();
		String firstDigit = String.valueOf(rand.nextInt(6)+1);
		String secondDigit = String.valueOf(rand.nextInt(6)+1);
		return "Brand#" + firstDigit + secondDigit;
	}

	private static final List<String> COLORS = new ArrayList<>(Arrays.asList(
			"almond", "antique", "aquamarine", "azure", "beige", "bisque", "black", "blanched", "blue",
			"blush", "brown", "burlywood", "burnished", "chartreuse", "chiffon", "chocolate", "coral",
			"cornflower", "cornsilk", "cream", "cyan", "dark", "deep", "dim", "dodger", "drab", "firebrick",
			"floral", "forest", "frosted", "gainsboro", "ghost", "goldenrod", "green", "grey", "honeydew",
			"hot", "indian", "ivory", "khaki", "lace", "lavender", "lawn", "lemon", "light", "lime", "linen",
			"magenta", "maroon", "medium", "metallic", "midnight", "mint", "misty", "moccasin", "navajo",
			"navy", "olive", "orange", "orchid", "pale", "papaya", "peach", "peru", "pink", "plum", "powder",
			"puff", "purple", "red", "rose", "rosy", "royal", "saddle", "salmon", "sandy", "seashell", "sienna",
			"sky", "slate", "smoke", "snow", "spring", "steel", "tan", "thistle", "tomato", "turquoise", "violet",
			"wheat", "white", "yellow"));

	public static String getRandomColor() {
		return getRandomElementFromList(COLORS);
	}

	public static double convertToTwoDecimal(final double value) {
		return Math.round(value * 100.0) / 100.0;
	}

	/**
	 * Get a random integer between two values (both are inclusive).
	 * 
	 * @param upperLimit
	 * @param bottomLimit
	 * @return
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
	 * @return
	 */
	public static double getRandomDouble(final double bottomLimit, final double upperLimit) {
		final Random rand = new Random();
		return bottomLimit + (upperLimit - bottomLimit) * rand.nextDouble();
	}


}
