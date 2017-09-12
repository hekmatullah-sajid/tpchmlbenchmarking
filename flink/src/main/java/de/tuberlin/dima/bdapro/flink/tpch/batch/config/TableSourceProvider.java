package de.tuberlin.dima.bdapro.flink.tpch.batch.config;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.sources.CsvTableSource;

/**
 * TableSourceProvider is used as a single table source provider for all 22 TPCH
 * queries.
 * 
 * @author Hekmatullah Sajid and Seema Narasimha Swamy
 *
 */
public class TableSourceProvider {

	private String baseDir = PathConfig.BASE_DIR;

	public String getBaseDir() {
		return baseDir;
	}

	/**
	 * @param path
	 *            is used in the main class to pass the path to directory where
	 *            data sets are located.
	 */
	public void setBaseDir(final String path) {
		baseDir = path;
	}

	/**
	 * Method used to read the data in the eight data sets by calling the right
	 * method, and register the TableSources in the ExecutionEnvironment.
	 * 
	 * @param env,
	 *            ExecutionEnvironment passed from main method to register the
	 *            TableSources.
	 * @param sf,
	 *            defines the scale factor of data sets.
	 * @return BatchTableEnvironment used in queries for executing the queries.
	 */
	public BatchTableEnvironment loadDataBatch(final ExecutionEnvironment env, final String sf) {
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

		tableEnv.registerTableSource("lineitem", readLineitem(sf));
		tableEnv.registerTableSource("supplier", readSupplier(sf));
		tableEnv.registerTableSource("part", readPart(sf));
		tableEnv.registerTableSource("partsupp", readPartsupp(sf));
		tableEnv.registerTableSource("customer", readCustomer(sf));
		tableEnv.registerTableSource("nation", readNation(sf));
		tableEnv.registerTableSource("region", readRegion(sf));
		tableEnv.registerTableSource("orders", readOrders(sf));

		return tableEnv;
	}

	/**
	 * method is used to read orders data set into CsvTableSource
	 */
	private CsvTableSource readOrders(final String sf) {
		return new CsvTableSource(baseDir + sf + "/" + PathConfig.ORDERS,
				new String[] { "o_orderkey", "o_custkey", "o_orderstatus", "o_totalprice", "o_orderdate",
						"o_orderpriority", "o_clerk", "o_shipriority", "o_comment" },
				new TypeInformation<?>[] { Types.INT(), Types.INT(), Types.STRING(), Types.DOUBLE(), Types.STRING(),
						Types.STRING(), Types.STRING(), Types.INT(), Types.STRING() },
				"|", "\n", null, false, null, false);
	}

	/**
	 * method is used to read region data set into CsvTableSource
	 */
	private CsvTableSource readRegion(final String sf) {
		return new CsvTableSource(baseDir + sf + "/" + PathConfig.REGION,
				new String[] { "r_regionkey", "r_name", "r_comment" },
				new TypeInformation<?>[] { Types.INT(), Types.STRING(), Types.STRING() }, "|", "\n", null, false, null,
				false);
	}

	/**
	 * method is used to read nation data set into CsvTableSource
	 */
	private CsvTableSource readNation(final String sf) {
		return new CsvTableSource(baseDir + sf + "/" + PathConfig.NATION,
				new String[] { "n_nationkey", "n_name", "n_regionkey", "n_comment" },
				new TypeInformation<?>[] { Types.INT(), Types.STRING(), Types.INT(), Types.STRING() }, "|", "\n", null,
				false, null, false);
	}

	/**
	 * method is used to read customer data set into CsvTableSource
	 */
	private CsvTableSource readCustomer(final String sf) {
		return new CsvTableSource(baseDir + sf + "/" + PathConfig.CUSTOMER,
				new String[] { "c_custkey", "c_name", "c_address", "c_nationkey", "c_phone", "c_acctbal",
						"c_mktsegment", "c_comment" },
				new TypeInformation<?>[] { Types.INT(), Types.STRING(), Types.STRING(), Types.INT(), Types.STRING(),
						Types.DOUBLE(), Types.STRING(), Types.STRING() },
				"|", "\n", null, false, null, false);
	}

	/**
	 * method is used to read partsupp data set into CsvTableSource
	 */
	private CsvTableSource readPartsupp(final String sf) {
		return new CsvTableSource(baseDir + sf + "/" + PathConfig.PARTSUPP,
				new String[] { "ps_partkey", "ps_suppkey", "ps_availqty", "ps_supplycost", "ps_comment" },
				new TypeInformation<?>[] { Types.INT(), Types.INT(), Types.INT(), Types.DOUBLE(), Types.STRING() }, "|",
				"\n", null, false, null, false);
	}

	/**
	 * method is used to read part data set into CsvTableSource
	 */
	private CsvTableSource readPart(final String sf) {
		return new CsvTableSource(baseDir + sf + "/" + PathConfig.PART,
				new String[] { "p_partkey", "p_name", "p_mfgr", "p_brand", "p_type", "p_size", "p_container",
						"p_retailprice", "p_comment" },
				new TypeInformation<?>[] { Types.INT(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(),
						Types.INT(), Types.STRING(), Types.DOUBLE(), Types.STRING() },
				"|", "\n", null, false, null, false);
	}

	/**
	 * method is used to read supplier data set into CsvTableSource
	 */
	private CsvTableSource readSupplier(final String sf) {
		return new CsvTableSource(baseDir + sf + "/" + PathConfig.SUPPLIER,
				new String[] { "s_suppkey", "s_name", "s_address", "s_nationkey", "s_phone", "s_acctbal", "s_comment" },
				new TypeInformation<?>[] { Types.INT(), Types.STRING(), Types.STRING(), Types.INT(), Types.STRING(),
						Types.DOUBLE(), Types.STRING() },
				"|", "\n", null, false, null, false);
	}

	/**
	 * method is used to read lineitem data set into CsvTableSource
	 */
	private CsvTableSource readLineitem(final String sf) {
		return new CsvTableSource(baseDir + sf + "/" + PathConfig.LINEITEM,
				new String[] { "l_orderkey", "l_partkey", "l_suppkey", "l_linenumber", "" + "l_quantity",
						"l_extendedprice", "l_discount", "l_tax", "l_returnflag", "l_linestatus", "l_shipdate",
						"l_commitdate", "l_receiptdate", "l_shipinstruct", "l_shipmode", "l_comment" },
				new TypeInformation<?>[] { Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.DOUBLE(),
						Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), Types.STRING(), Types.STRING(), Types.STRING(),
						Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING() },
				"|", "\n", null, false, null, false);
	}

}
