package de.tuberlin.dima.bdapro.flink.tpch.streaming;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.junit.Before;


public class QueriesTest {

	private ExecutionEnvironment env;

	@Before
	public void setUp() throws Exception {
		env = ExecutionEnvironment.getExecutionEnvironment();
	}

}
