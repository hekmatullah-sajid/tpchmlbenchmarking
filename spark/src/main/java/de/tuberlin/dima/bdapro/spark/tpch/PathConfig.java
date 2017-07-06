package de.tuberlin.dima.bdapro.spark.tpch;

import java.io.File;

public class PathConfig {

	private static final String PARENT_DIR = (new File(System.getProperty("user.dir"))).getParent();

	public static final String BASE_DIR = PARENT_DIR + "/tpch2.17.2/dbgen/testdata/";

	////// TABLE NAMES //////
	public static final String LINEITEM = "lineitem.tbl";

	public static final String NATION = "nation.tbl";

	public static final String CUSTOMER = "customer.tbl";

	public static final String ORDERS = "orders.tbl";

	public static final String SUPPLIER = "supplier.tbl";

	public static final String REGION = "region.tbl";

	public static final String PART = "part.tbl";

	public static final String PARTSUPP = "partsupp.tbl";

}
