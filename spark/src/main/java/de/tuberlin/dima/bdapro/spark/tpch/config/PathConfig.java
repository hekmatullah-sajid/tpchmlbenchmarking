package de.tuberlin.dima.bdapro.spark.tpch.config;

import java.io.File;

/**
 * <p> The PthaConfig class is used to configure the path to directory where data sets are located.
 * All variables of the class is static. </p> 
 * @author Hekmatullah Sajid and Seema Narasimha Swamy
 */
public class PathConfig {
	
	/**
	 *  Refers to the parent directory of execution directory.
	 */
	private static final String PARENT_DIR = (new File(System.getProperty("user.dir"))).getParent();
	
	/**
	 * Refers to the directory where the data sets are located.
	 * PARENT_DIR + "/tpch2.17.2/dbgen/testdata/" is used as default directory used to store data sets for JUnit tests.
	 */
	public static final String BASE_DIR = PARENT_DIR + "/tpch2.17.2/dbgen/testdata/";

	/*
	 *  The following variables define the name of each data set used in TPCH queries as input data
	 */
	public static final String LINEITEM = "lineitem.tbl";

	public static final String NATION = "nation.tbl";

	public static final String CUSTOMER = "customer.tbl";

	public static final String ORDERS = "orders.tbl";

	public static final String SUPPLIER = "supplier.tbl";

	public static final String REGION = "region.tbl";

	public static final String PART = "part.tbl";

	public static final String PARTSUPP = "partsupp.tbl";

}
