package de.tuberlin.dima.bdapro.flink.tpch;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sources.CsvTableSource;

public class TableSourceProvider {

	private String baseDir = PathConfig.BASE_DIR;

	public String getBaseDir(){
		return baseDir;
	}

	public void setBaseDir(final String path){
		baseDir = path;
	}

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

	public StreamTableEnvironment loadDataStream(final StreamExecutionEnvironment env, final String sf) {
		StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

		// TODO CHANGE TO KAFKA!!!!
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

	private CsvTableSource readOrders(final String sf) {
		// read orders
		return new CsvTableSource(baseDir + sf + "/" + PathConfig.ORDERS,
				new String[] { "o_orderkey", "o_custkey", "o_orderstatus", "o_totalprice", "o_orderdate",
						"o_orderpriority", "o_clerk", "o_shipriority", "o_comment" },
				new TypeInformation<?>[] { Types.INT(), Types.INT(), Types.STRING(), Types.DOUBLE(), Types.STRING(),
			Types.STRING(), Types.STRING(), Types.INT(), Types.STRING() },
				"|", "\n", null, false, null, false);
	}

	private CsvTableSource readRegion(final String sf) {
		// read region
		return new CsvTableSource(baseDir + sf + "/" + PathConfig.REGION,
				new String[] { "r_regionkey", "r_name", "r_comment" },
				new TypeInformation<?>[] { Types.INT(), Types.STRING(), Types.STRING() }, "|", "\n", null, false, null,
				false);
	}

	private CsvTableSource readNation(final String sf) {
		// read nation
		return new CsvTableSource(baseDir + sf + "/" + PathConfig.NATION,
				new String[] { "n_nationkey", "n_name", "n_regionkey", "n_comment" },
				new TypeInformation<?>[] { Types.INT(), Types.STRING(), Types.INT(), Types.STRING() }, "|", "\n", null,
				false, null, false);
	}

	private CsvTableSource readCustomer(final String sf) {
		// read customer
		return new CsvTableSource(baseDir + sf + "/" + PathConfig.CUSTOMER,
				new String[] { "c_custkey", "c_name", "c_address", "c_nationkey", "c_phone", "c_acctbal",
						"c_mktsegment", "c_comment" },
				new TypeInformation<?>[] { Types.INT(), Types.STRING(), Types.STRING(), Types.INT(), Types.STRING(),
			Types.DOUBLE(), Types.STRING(), Types.STRING() },
				"|", "\n", null, false, null, false);
	}

	private CsvTableSource readPartsupp(final String sf) {
		// read partsupp
		return new CsvTableSource(baseDir + sf + "/" + PathConfig.PARTSUPP,
				new String[] { "ps_partkey", "ps_suppkey", "ps_availqty", "ps_supplycost", "ps_comment" },
				new TypeInformation<?>[] { Types.INT(), Types.INT(), Types.INT(), Types.DOUBLE(), Types.STRING() }, "|",
				"\n", null, false, null, false);
	}

	private CsvTableSource readPart(final String sf) {
		// read part
		return new CsvTableSource(baseDir + sf + "/" + PathConfig.PART,
				new String[] { "p_partkey", "p_name", "p_mfgr", "p_brand", "p_type", "p_size", "p_container",
						"p_retailprice", "p_comment" },
				new TypeInformation<?>[] { Types.INT(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(),
			Types.INT(), Types.STRING(), Types.DOUBLE(), Types.STRING() },
				"|", "\n", null, false, null, false);
	}

	private CsvTableSource readSupplier(final String sf) {
		// read supplier
		return new CsvTableSource(baseDir + sf + "/" + PathConfig.SUPPLIER,
				new String[] { "s_suppkey", "s_name", "s_address", "s_nationkey", "s_phone", "s_acctbal", "s_comment" },
				new TypeInformation<?>[] { Types.INT(), Types.STRING(), Types.STRING(), Types.INT(), Types.STRING(),
			Types.DOUBLE(), Types.STRING() },
				"|", "\n", null, false, null, false);
	}

	private CsvTableSource readLineitem(final String sf) {
		// read lineitem
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
