package de.tuberlin.dima.bdapro.flink.tpch.batch.queries;

import de.tuberlin.dima.bdapro.flink.tpch.Utils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;

import java.util.List;

/**
 * Created by seema on 06/06/2017.
 */
public class Query17 extends Query {
    public Query17(BatchTableEnvironment env) {
        super(env);
    }

    @Override
    public List<Tuple1<Double>> execute() {
        return execute(Utils.getRandomBrand(), Utils.getRandomContainer());
    }

    public List<Tuple1<Double>> execute(String brand, String container) {
        String SQLQuery = "SELECT sum(l_extendedprice) / 7.0 as avg_yearly "
                + "FROM lineitem, part "
                + "WHERE p_partkey = l_partkey and p_brand = '" + brand + "' "
                + "and p_container = '" + container + "' "
                + "and l_quantity < ( "
                + "select 0.2 * avg(l_quantity) from lineitem "
                + "where l_partkey = p_partkey ) ";

        Table res = env.sql(SQLQuery);

        try {
            return env.toDataSet(res, TypeInformation.of(new TypeHint<Tuple1<Double>>() {
            })).map(new MapFunction<Tuple1<Double>, Tuple1<Double>>() {
                private static final long serialVersionUID = 1L;

                @Override
                public Tuple1<Double> map(final Tuple1<Double> value) throws Exception {
                    return Utils.keepOnlyTwoDecimals(value);
                }
            }).collect();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }
}
