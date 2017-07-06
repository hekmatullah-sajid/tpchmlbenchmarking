package de.tuberlin.dima.bdapro.flink.tpch.batch.queries;

import java.util.List;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.table.api.java.BatchTableEnvironment;

public abstract class Query {

	protected final BatchTableEnvironment env;

	public Query(final BatchTableEnvironment env) {
		this.env = env;		
	}

	public abstract List<? extends Tuple> execute();

}
