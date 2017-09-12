package de.tuberlin.dima.bdapro.flink.tpch.batch.queries;

import java.util.List;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.table.api.java.BatchTableEnvironment;

/**
 * The blueprint class for all queries which they extends this class.
 * 
 * @author Hekmatullah Sajid and Seema Narasimha Swamy
 *
 */
public abstract class Query {

	protected final BatchTableEnvironment env;

	public Query(final BatchTableEnvironment env) {
		this.env = env;
	}

	/**
	 * All query classes implement the execute blueprint abstract execute method
	 * to execute the TPCH SQL query. This method generates substitution
	 * parameters that must be generated and used to build the executable query
	 * text. The override execute method than calls the query's execute method
	 * (with parameters) and passes the random values.
	 */
	public abstract List<? extends Tuple> execute();

}
