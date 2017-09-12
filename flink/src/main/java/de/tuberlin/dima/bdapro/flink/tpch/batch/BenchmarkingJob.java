package de.tuberlin.dima.bdapro.flink.tpch.batch;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;

import de.tuberlin.dima.bdapro.flink.tpch.batch.config.TableSourceProvider;
import de.tuberlin.dima.bdapro.flink.tpch.batch.queries.Query;
import de.tuberlin.dima.bdapro.flink.tpch.batch.queries.Query1;
import de.tuberlin.dima.bdapro.flink.tpch.batch.queries.Query10;
import de.tuberlin.dima.bdapro.flink.tpch.batch.queries.Query11;
import de.tuberlin.dima.bdapro.flink.tpch.batch.queries.Query12;
import de.tuberlin.dima.bdapro.flink.tpch.batch.queries.Query13;
import de.tuberlin.dima.bdapro.flink.tpch.batch.queries.Query14;
import de.tuberlin.dima.bdapro.flink.tpch.batch.queries.Query15;
import de.tuberlin.dima.bdapro.flink.tpch.batch.queries.Query16;
import de.tuberlin.dima.bdapro.flink.tpch.batch.queries.Query17;
import de.tuberlin.dima.bdapro.flink.tpch.batch.queries.Query18;
import de.tuberlin.dima.bdapro.flink.tpch.batch.queries.Query19;
import de.tuberlin.dima.bdapro.flink.tpch.batch.queries.Query2;
import de.tuberlin.dima.bdapro.flink.tpch.batch.queries.Query20;
import de.tuberlin.dima.bdapro.flink.tpch.batch.queries.Query21;
import de.tuberlin.dima.bdapro.flink.tpch.batch.queries.Query22;
import de.tuberlin.dima.bdapro.flink.tpch.batch.queries.Query3;
import de.tuberlin.dima.bdapro.flink.tpch.batch.queries.Query4;
import de.tuberlin.dima.bdapro.flink.tpch.batch.queries.Query5;
import de.tuberlin.dima.bdapro.flink.tpch.batch.queries.Query6;
import de.tuberlin.dima.bdapro.flink.tpch.batch.queries.Query7;
import de.tuberlin.dima.bdapro.flink.tpch.batch.queries.Query8;
import de.tuberlin.dima.bdapro.flink.tpch.batch.queries.Query9;

/**
 * How to use: ./bin/flink run benchmarkingJob.jar <path//to//the//testDB>
 * <//scaleFactor//> <//parallelism//> example: ./bin/flink run benchmarkingJob.jar
 * /home/ubuntu/tpch/dbgen/testdata 1.0 150
 *
 */
/**
 * Main class of the project for benchmarking TPCH queries on Apache Spark. How
 * to run the jar on Flink: ./bin/flink run path-to-jar path-to-datasets scale
 * 
 * @author Hekmatullah Sajid and Seema Narasimha Swamy
 *
 */
public class BenchmarkingJob {

	public static void main(final String[] args) {
		/**
		 * ARGUMENT PARSING The main method expects maximum two arguments. First
		 * argument should be the path to directory where data sets are located
		 * Second argument should be the scale factor of datasets
		 */
		if (args.length <= 0 || args.length > 2) {
			throw new IllegalArgumentException(
					"Please input the path to the directory where the test databases are located.");
		}
		String path = args[0];
		try {
			File file = new File(path);
			if (!(file.isDirectory() && file.exists() || path.contains("hdfs"))) {
				throw new IllegalArgumentException(
						"Please give a valid path to the directory where the test databases are located.");
			}

		} catch (Exception ex) {
			throw new IllegalArgumentException(
					"Please give a valid path to the directory where the test databases are located.");
		}

		String sf = args[1];
		try {
			Double.parseDouble(sf);
		} catch (Exception ex) {
			throw new IllegalArgumentException("Please give a valid scale factor.");
		}

		/*
		 * Creating table source provider and setting the base directory where
		 * data sets are located. And loading the data from data sets into
		 * CSVTableReader and registering them in BatchTableEnvironment.
		 */
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		TableSourceProvider provider = new TableSourceProvider();
		provider.setBaseDir(path);
		BatchTableEnvironment tableEnv = provider.loadDataBatch(env, sf);

		/*
		 * The results list is used to store the execution time for each query.
		 * The start and end variables are used to store the starting and ending
		 * time of executing a query, the difference of these two is written to
		 * the result with its corresponding query name
		 */
		List<String> results = new ArrayList<String>();
		long start = 0;
		long end = 0;

		/*
		 * Benchmarking the queries for their execution time.
		 */
		start = System.currentTimeMillis();
		final Query q1 = new Query1(tableEnv);
		q1.execute();
		end = System.currentTimeMillis();
		results.add(" Query1, " + (end - start) + "\r\n");

		start = System.currentTimeMillis();
		final Query q2 = new Query2(tableEnv);
		q2.execute();
		end = System.currentTimeMillis();
		results.add(" Query2, " + (end - start) + "\r\n");

		start = System.currentTimeMillis();
		final Query q3 = new Query3(tableEnv);
		q3.execute();
		end = System.currentTimeMillis();
		results.add(" Query3, " + (end - start) + "\r\n");

		start = System.currentTimeMillis();
		final Query q4 = new Query4(tableEnv);
		q4.execute();
		end = System.currentTimeMillis();
		results.add(" Query4, " + (end - start) + "\r\n");

		start = System.currentTimeMillis();
		final Query q5 = new Query5(tableEnv);
		q5.execute();
		end = System.currentTimeMillis();
		results.add(" Query5, " + (end - start) + "\r\n");

		start = System.currentTimeMillis();
		final Query q6 = new Query6(tableEnv);
		q6.execute();
		end = System.currentTimeMillis();
		results.add(" Query6, " + (end - start) + "\r\n");

		start = System.currentTimeMillis();
		final Query q7 = new Query7(tableEnv);
		q7.execute();
		end = System.currentTimeMillis();
		results.add(" Query7, " + (end - start) + "\r\n");

		start = System.currentTimeMillis();
		final Query q8 = new Query8(tableEnv);
		q8.execute();
		end = System.currentTimeMillis();
		results.add(" Query8, " + (end - start) + "\r\n");

		start = System.currentTimeMillis();
		final Query q9 = new Query9(tableEnv);
		q9.execute();
		end = System.currentTimeMillis();
		results.add(" Query9, " + (end - start) + "\r\n");

		start = System.currentTimeMillis();
		final Query q10 = new Query10(tableEnv);
		q10.execute();
		end = System.currentTimeMillis();
		results.add(" Query10, " + (end - start) + "\r\n");

		start = System.currentTimeMillis();
		final Query q11 = new Query11(tableEnv, "1.0");
		q11.execute();
		end = System.currentTimeMillis();
		results.add(" Query11, " + (end - start) + "\r\n");

		start = System.currentTimeMillis();
		final Query q12 = new Query12(tableEnv);
		q12.execute();
		end = System.currentTimeMillis();
		results.add(" Query12, " + (end - start) + "\r\n");

		start = System.currentTimeMillis();
		final Query q13 = new Query13(tableEnv);
		q13.execute();
		end = System.currentTimeMillis();
		results.add(" Query13, " + (end - start) + "\r\n");

		start = System.currentTimeMillis();
		final Query q14 = new Query14(tableEnv);
		q14.execute();
		end = System.currentTimeMillis();
		results.add(" Query14, " + (end - start) + "\r\n");

		start = System.currentTimeMillis();
		final Query q15 = new Query15(tableEnv);
		q15.execute();
		end = System.currentTimeMillis();
		results.add(" Query15, " + (end - start) + "\r\n");

		start = System.currentTimeMillis();
		final Query q16 = new Query16(tableEnv);
		q16.execute();
		end = System.currentTimeMillis();
		results.add(" Query16, " + (end - start) + "\r\n");

		start = System.currentTimeMillis();
		final Query q17 = new Query17(tableEnv);
		q17.execute();
		end = System.currentTimeMillis();
		results.add(" Query17, " + (end - start) + "\r\n");

		start = System.currentTimeMillis();
		final Query q18 = new Query18(tableEnv);
		q18.execute();
		end = System.currentTimeMillis();
		results.add(" Query18, " + (end - start) + "\r\n");

		start = System.currentTimeMillis();
		final Query q19 = new Query19(tableEnv);
		q19.execute();
		end = System.currentTimeMillis();
		results.add(" Query19, " + (end - start) + "\r\n");

		start = System.currentTimeMillis();
		final Query q20 = new Query20(tableEnv);
		q20.execute();
		end = System.currentTimeMillis();
		results.add(" Query20, " + (end - start) + "\r\n");

		start = System.currentTimeMillis();
		final Query q21 = new Query21(tableEnv);
		q21.execute();
		end = System.currentTimeMillis();
		results.add(" Query21, " + (end - start) + "\r\n");

		start = System.currentTimeMillis();
		final Query q22 = new Query22(tableEnv);
		q22.execute();
		end = System.currentTimeMillis();
		results.add(" Query22, " + (end - start) + "\r\n");

		/**
		 * Write the benchmarking values stored in results list to output file.
		 */
		try {
			FileWriter writer = new FileWriter("FlinkBatchOutput.txt", true);
			for (String str : results) {
				writer.write(str);
			}
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.exit(0);

	}

}
