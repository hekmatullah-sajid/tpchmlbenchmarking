package de.tuberlin.dima.bdapro.spark.tpch.batch;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.SparkSession;

import de.tuberlin.dima.bdapro.spark.tpch.batch.queries.Query;
import de.tuberlin.dima.bdapro.spark.tpch.batch.queries.Query1;
import de.tuberlin.dima.bdapro.spark.tpch.batch.queries.Query10;
import de.tuberlin.dima.bdapro.spark.tpch.batch.queries.Query11;
import de.tuberlin.dima.bdapro.spark.tpch.batch.queries.Query12;
import de.tuberlin.dima.bdapro.spark.tpch.batch.queries.Query13;
import de.tuberlin.dima.bdapro.spark.tpch.batch.queries.Query14;
import de.tuberlin.dima.bdapro.spark.tpch.batch.queries.Query15;
import de.tuberlin.dima.bdapro.spark.tpch.batch.queries.Query16;
import de.tuberlin.dima.bdapro.spark.tpch.batch.queries.Query17;
import de.tuberlin.dima.bdapro.spark.tpch.batch.queries.Query18;
import de.tuberlin.dima.bdapro.spark.tpch.batch.queries.Query19;
import de.tuberlin.dima.bdapro.spark.tpch.batch.queries.Query2;
import de.tuberlin.dima.bdapro.spark.tpch.batch.queries.Query20;
import de.tuberlin.dima.bdapro.spark.tpch.batch.queries.Query21;
import de.tuberlin.dima.bdapro.spark.tpch.batch.queries.Query22;
import de.tuberlin.dima.bdapro.spark.tpch.batch.queries.Query3;
import de.tuberlin.dima.bdapro.spark.tpch.batch.queries.Query4;
import de.tuberlin.dima.bdapro.spark.tpch.batch.queries.Query5;
import de.tuberlin.dima.bdapro.spark.tpch.batch.queries.Query6;
import de.tuberlin.dima.bdapro.spark.tpch.batch.queries.Query7;
import de.tuberlin.dima.bdapro.spark.tpch.batch.queries.Query8;
import de.tuberlin.dima.bdapro.spark.tpch.batch.queries.Query9;

public class BenchmarkingJob {

	public static void main(final String[] args) {
		
////////////////////////ARGUMENT PARSING ///////////////////////////////
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
		 * Benchmarking the queries for their execution time.
		 */
		SparkSession spark = SparkSession.builder()
				.appName("TPCH Spark Batch Benchmarking").getOrCreate();
		
		TableSourceProvider provider = new TableSourceProvider();
		provider.setBaseDir(path);
		
		spark = provider.loadData(spark, sf);
		
		// Variables to store Benchmarking intermediate and final values 
		List<String> results = new ArrayList<String>();
		long start = 0;
		long end = 0;
		
		start = System.currentTimeMillis();
		final Query q1 = new Query1(spark);
		q1.execute();
		end = System.currentTimeMillis();
		results.add(" Query1|" + (end - start) + "\r\n");
		
		start = System.currentTimeMillis();
		final Query q2 = new Query2(spark);
		q2.execute();
		end = System.currentTimeMillis();
		results.add(" Query2|" + (end - start) + "\r\n");
		
		start = System.currentTimeMillis();
		final Query q3 = new Query3(spark);
		q3.execute();
		end = System.currentTimeMillis();
		results.add(" Query3|" + (end - start) + "\r\n");
		
		start = System.currentTimeMillis();
		final Query q4 = new Query4(spark);
		q4.execute();
		end = System.currentTimeMillis();
		results.add(" Query4|" + (end - start) + "\r\n");
		
		start = System.currentTimeMillis();
		final Query q5 = new Query5(spark);
		q5.execute();
		end = System.currentTimeMillis();
		results.add(" Query5|" + (end - start) + "\r\n");
		
		start = System.currentTimeMillis();
		final Query q6 = new Query6(spark);
		q6.execute();
		end = System.currentTimeMillis();
		results.add(" Query6|" + (end - start) + "\r\n");
		
		start = System.currentTimeMillis();
		final Query q7 = new Query7(spark);
		q7.execute();
		end = System.currentTimeMillis();
		results.add(" Query7|" + (end - start) + "\r\n");
		
		start = System.currentTimeMillis();
		final Query q8 = new Query8(spark);
		q8.execute();
		end = System.currentTimeMillis();
		results.add(" Query8|" + (end - start) + "\r\n");
		
		start = System.currentTimeMillis();
		final Query q9 = new Query9(spark);
		q9.execute();
		end = System.currentTimeMillis();
		results.add(" Query9|" + (end - start) + "\r\n");
		
		start = System.currentTimeMillis();
		final Query q10 = new Query10(spark);
		q10.execute();
		end = System.currentTimeMillis();
		results.add(" Query10|" + (end - start) + "\r\n");
		
		start = System.currentTimeMillis();
		final Query q11 = new Query11(spark, "1.0");
		q11.execute();
		end = System.currentTimeMillis();
		results.add(" Query11|" + (end - start) + "\r\n");
		
		start = System.currentTimeMillis();
		final Query q12 = new Query12(spark);
		q12.execute();
		end = System.currentTimeMillis();
		results.add(" Query12|" + (end - start) + "\r\n");
		
		start = System.currentTimeMillis();
		final Query q13 = new Query13(spark);
		q13.execute();
		end = System.currentTimeMillis();
		results.add(" Query13|" + (end - start) + "\r\n");
		
		start = System.currentTimeMillis();
		final Query q14 = new Query14(spark);
		q14.execute();
		end = System.currentTimeMillis();
		results.add(" Query14|" + (end - start) + "\r\n");
		
		start = System.currentTimeMillis();
		final Query q15 = new Query15(spark);
		q15.execute();
		end = System.currentTimeMillis();
		results.add(" Query15|" + (end - start) + "\r\n");
		
		start = System.currentTimeMillis();
		final Query q16 = new Query16(spark);
		q16.execute();
		end = System.currentTimeMillis();
		results.add(" Query16|" + (end - start) + "\r\n");
		
		start = System.currentTimeMillis();
		final Query q17 = new Query17(spark);
		q17.execute();
		end = System.currentTimeMillis();
		results.add(" Query17|" + (end - start) + "\r\n");
		
		start = System.currentTimeMillis();
		final Query q18 = new Query18(spark);
		q18.execute();
		end = System.currentTimeMillis();
		results.add(" Query18|" + (end - start) + "\r\n");
		
		start = System.currentTimeMillis();
		final Query q19 = new Query19(spark);
		q19.execute();
		end = System.currentTimeMillis();
		results.add(" Query19|" + (end - start) + "\r\n");
		
		start = System.currentTimeMillis();
		final Query q20 = new Query20(spark);
		q20.execute();
		end = System.currentTimeMillis();
		results.add(" Query20|" + (end - start) + "\r\n");
		
//		start = System.currentTimeMillis();
//		final Query q21 = new Query21(spark);
//		q21.execute();
//		end = System.currentTimeMillis();
//		results.add(" Query21|" + (end - start) + "\r\n");
		
		start = System.currentTimeMillis();
		final Query q22 = new Query22(spark);
		q22.execute();
		end = System.currentTimeMillis();
		results.add(" Query22|" + (end - start) + "\r\n");

		////////////////////////WRITE OUTPUT TO FILE ///////////////////////////////
		
		// TODO write the output in distributed fashion!!!
		try {
			FileWriter writer = new FileWriter("SparkBatchOutput.txt", true);
			for (String str : results) {
				writer.write(str);
			}
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
