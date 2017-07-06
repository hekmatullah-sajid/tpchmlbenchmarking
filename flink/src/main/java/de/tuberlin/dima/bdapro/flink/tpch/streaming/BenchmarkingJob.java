package de.tuberlin.dima.bdapro.flink.tpch.streaming;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import de.tuberlin.dima.bdapro.flink.tpch.TableSourceProvider;
import de.tuberlin.dima.bdapro.flink.tpch.streaming.queries.Query1;

public class BenchmarkingJob {

	public static void main(final String[] args) {
		String sf = "1.0";
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		TableSourceProvider provider = new TableSourceProvider();
		provider.setBaseDir("");
		StreamTableEnvironment tableEnv = provider.loadDataStream(env, sf);

		long start = System.currentTimeMillis();
		System.out.println("start: " + start);
		final Query1 q13 = new Query1(tableEnv, sf);
		q13.execute();
		long end = System.currentTimeMillis();
		System.out.println("end: " + end);
		System.out.println("diff: " + (end-start));
	}

}
