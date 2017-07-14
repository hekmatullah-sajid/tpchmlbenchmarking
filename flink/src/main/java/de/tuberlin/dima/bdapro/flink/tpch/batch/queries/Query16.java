package de.tuberlin.dima.bdapro.flink.tpch.batch.queries;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;

import de.tuberlin.dima.bdapro.flink.tpch.Utils;

/**
 * Created by seema on 06/06/2017.
 */
public class Query16 extends Query {
    public Query16(BatchTableEnvironment env) {
        super(env);
    }

    @Override
    public List<Tuple4<String, String, Integer, Long>> execute() {
        return execute(Utils.getRandomBrand(),Utils.getRandomType(), getRandomSize());
    }

    public List<Tuple4<String, String, Integer, Long>> execute(String brand, String type, List<Integer> sizeArray) {
        Integer size1 = sizeArray.get(0);
        Integer size2 = sizeArray.get(1);
        Integer size3 = sizeArray.get(2);
        Integer size4 = sizeArray.get(3);
        Integer size5 = sizeArray.get(4);
        Integer size6 = sizeArray.get(5);
        Integer size7 = sizeArray.get(6);
        Integer size8 = sizeArray.get(7);

        Table partsupp_mod = env.scan("partsupp").select("ps_partkey,ps_suppkey").distinct();
        env.registerTable("partsupp_mod", partsupp_mod);

        String SQLQuery = "SELECT p_brand, p_type, p_size, count(ps_suppkey) as supplier_cnt "
                + "FROM partsupp_mod, part "
                + "WHERE p_partkey = ps_partkey "
                + "AND p_brand <> '" + brand + "' " + "AND  p_type not like '" + type +"%' "
                + "AND p_size IN ( " + size1 + ", " + size2 + ", " + size3 + ", " + size4 + ", "
                                + size5 + ", " + size6 + ", " + size7 + ", " + size8 + " ) "
                + "AND NOT EXISTS(  "
                + "SELECT s_suppkey FROM supplier "
                + "WHERE s_comment like '%Customer%Complaints%' AND s_suppkey = ps_suppkey) "
                + "GROUP BY p_brand, p_type, p_size ORDER BY supplier_cnt desc, p_brand, p_type, p_size ";

        Table res = env.sql(SQLQuery);

//        Table supplier = env.scan("supplier").filter("LIKE(s_comment,'%Customer%Complaints%') " );
//        Table partsupp = env.scan("partsupp").select("ps_partkey,ps_suppkey").distinct();
//        Table part = env.scan("part").filter("p_brand <> '" + brand + "' ")
//                .where("p_type <> '" + type + "%' ")
//                .where("p_size = " + sizeArray.get(0) + "|| p_size = " +  sizeArray.get(1) +  "|| p_size = " + sizeArray.get(2)
//                        + "||p_size = " + sizeArray.get(3) + "|| p_size = " + sizeArray.get(4) + "|| p_size = " + sizeArray.get(5)
//                        +"|| p_size = " + sizeArray.get(6) + "||p_size = " + sizeArray.get(7));
//
//        Table innerquery = partsupp.join(supplier).where("ps_suppkey != s_suppkey");
//        Table outerQ = part.join(innerquery).where("p_partkey = ps_partkey")
//                .groupBy("p_brand, p_type, p_size")
//                .select("p_brand, p_type, p_size, count(ps_suppkey) as supplier_cnt")
//                .orderBy("supplier_cnt , p_brand, p_type, p_size");


        try {
            return env.toDataSet(res, TypeInformation.of(new TypeHint<Tuple4<String, String, Integer, Long>>() {
            })).collect();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    private List<Integer> getRandomSize()
    {
        List<Integer> sizeArray = new ArrayList<>(8);
        for(int i = 0; i < 8; i++){
        sizeArray.add(Utils.getRandomInt(1,50));}
        return sizeArray;

    }
}
