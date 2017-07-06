package de.tuberlin.dima.bdapro.flink.tpch.streaming.queries;

import org.apache.flink.table.api.java.StreamTableEnvironment;

public abstract class Query {

	protected final StreamTableEnvironment env;
	protected final String sf;

	public Query(final StreamTableEnvironment env, final String sf) {
		this.env = env;
		this.sf = sf;
	}

	public abstract void execute();

}
