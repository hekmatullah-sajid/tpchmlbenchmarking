/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.quickstart;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.functions.TimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.triggers.PurgingTrigger;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.util.Random;
import java.util.concurrent.TimeUnit;


/**
 * This program implements a modified version of the simple TPC-H query
 * The original query can be found at
 * <a href="http://www.tpc.org/tpch/spec/tpch2.16.0.pdf">http://www.tpc.org/tpch/spec/tpch2.16.0.pdf</a> (page 45).
 *
 * <p>
 * This program implements the following SQL equivalent:
 *
 * <p>
 * <pre>{@code
 * SELECT
 *        x.l_quantity, SUM(y.o_totalprice) AS revenue,
 * FROM
 *        x in lineitem, y in orders,
 * WHERE
 *        x.l_orderkey = y.o_orderkey
 * GROUP BY
 *        x.l_quantity
 * }</pre>
 *
 * <p>
 * Usage: <code>TPCHOrderLineitem --lineitem&lt;path&gt; --orders &lt;path&gt </code><br>
 *
 * <p>
  */
@SuppressWarnings("serial")
public class TPCHOrderLineitem {

    // *************************************************************************
    //     PROGRAM
    // *************************************************************************

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        if ( !params.has("orders") && !params.has("lineitem")) {
            System.err.println("  Usage: TPCHQuery10 --orders <path> --lineitem <path>  [--output <path>]");
            return;
        }
        final String inputOrders = params.getRequired("orders");
        final String inputLineitem = params.getRequired("lineitem");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //    env.enableCheckpointing(1000);

     /*   TextInputFormat inputFormat =
                new TextInputFormat(new Path(inputOrders));
*/


        // get orders data set: (orderkey, totalprice)
        DataStream<Tuple3<Long, String, Double>> orders = env.
                readTextFile(inputOrders).
                map(new OrderSplit());

        // get lineitem data set: (orderkey, quantity)
        DataStream<Tuple3<Long, String, Integer>> lineitems = env.
                readTextFile(inputLineitem).
                map(new LineitemSplit());

        // extract the timestamps
        orders = orders.assignTimestamps(new MyTimestampExtractor1());
        lineitems = lineitems.assignTimestamps(new MyTimestampExtractor2());

        // ContinuousProcessingTimeTrigger.of(Time.minutes(40));
        // join orders with lineitems: (totalprice, quantity)
        DataStream<Tuple3<String, Double, Integer>> revenueByQuantity =
                orders.join(lineitems)
                        .where(new NameKeySelector1()).equalTo(new NameKeySelector2())
                        .window(TumblingTimeWindows.of(Time.of(500000, TimeUnit.MILLISECONDS)))//GlobalWindows.create())//.trigger(PurgingTrigger.of(CountTrigger.of(500)))//TumblingProcessingTimeWindows.of(Time.of(5, TimeUnit.MILLISECONDS)))
                        .apply(new MyJoinFunction());//} JoinFunction<Tuple3<Long, String, Double>, Tuple3<Long, String, Integer>, Tuple4<Long, String, Double, Integer>>() {
/*
                            @Override
                            public Tuple4<Long, String, Double, Integer> join(
                                    Tuple3<Long, String, Double> first,
                                    Tuple3<Long, String, Integer> second) {
                                return new Tuple4<>(first.f0, first.f1, first.f2, second.f2);
                            }
                        });
*/
        revenueByQuantity = revenueByQuantity.keyBy(2).sum(1).project(2,1);
        revenueByQuantity.print();
        // emit result
   /*   // emit result
		if (params.has("output")) {
			revenueByQuantity.writeAsText(params.get("output"), 1);
		} else {
			revenueByQuantity.print();
        }
*/






env.execute("joining stream");

        /*
        // get orders data set: (orderkey, totalprice)
        DataSet<Tuple2<Integer, Double>> orders =
                getOrdersDataSet(env, params.get("orders"));
        // get lineitem data set: (orderkey, quantity)
        DataSet<Tuple2<Integer, Integer>> lineitems =
                getLineitemDataSet(env, params.get("lineitem"));


        // join orders with lineitems: (totalprice, quantity)
        DataSet<Tuple2<Integer, Double>> revenueByQuantity =
                orders.joinWithHuge(lineitems)
                        .where(0).equalTo(0)
                        .projectFirst(1).projectSecond(1);

        revenueByQuantity = revenueByQuantity.groupBy(1).aggregate(Aggregations.SUM, 0);


        // emit result
        if (params.has("output")) {
            revenueByQuantity.writeAsCsv(params.get("output"), "\n", "|");
            // execute program
            env.execute("TPCHOrderLineitem Simple Query ");
        } else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            revenueByQuantity.print();
        }
*/
    }

    // *************************************************************************
    //     UTIL METHODS
    // *************************************************************************

    private final static int SLEEP_TIME = 10;

/*
    private static DataSet<Tuple2<Integer, Double>> getOrdersDataSet(ExecutionEnvironment env, String ordersPath) {
        return env.readCsvFile(ordersPath)
                .fieldDelimiter("|")
                .includeFields("100100000")
                .types(Integer.class, Double.class);
    }

    private static DataSet<Tuple2<Integer, Integer>> getLineitemDataSet(ExecutionEnvironment env, String lineitemPath) {
        return env.readCsvFile(lineitemPath)
                .fieldDelimiter("|")
                .includeFields("1000100000000000")
                .types(Integer.class, Integer.class);
    }
  public static final class OrderSplit implements FlatMapFunction<String, Tuple3<Long, String, Double>> {
        private static final long serialVersionUID = 1L;
        private Random rand = new Random();
        @Override
        public void flatMap(String value, Collector<Tuple3<Long, String, Double>> out)
                throws Exception {
            // normalize and split the line
            String[] tokens = value.toLowerCase().split("\n");
            String [] tok;
            // emit the pairs
            for (String token : tokens) {
                if (token.length() > 0) {
                    tok = token.split("\\|");
                    Thread.sleep(rand.nextInt(SLEEP_TIME) + 1);
                    out.collect(new Tuple3<>(System.currentTimeMillis(), tok[0], Double.parseDouble(tok[3])));
                }
            }
        }
    }
    private static final class LineitemSplit implements FlatMapFunction<String, Tuple3<Long, String, Integer>> {
        private static final long serialVersionUID = 1L;
        private Random rand = new Random();
        @Override
        public void flatMap(String value, Collector<Tuple3<Long,String, Integer>> out)
                throws Exception {
            // normalize and split the line
            String[] tokens = value.toLowerCase().split("\n");
            String [] tok;
            // emit the pairs
            for (String token : tokens) {
                if (token.length() > 0) {
                    tok = token.split("\\|");
                //    Thread.sleep(rand.nextInt(SLEEP_TIME) + 1);
                    out.collect(new Tuple3<>(System.currentTimeMillis(),tok[0], Integer.parseInt(tok[4])));
                }
            }
        }
    }*/

    public static class OrderSplit extends RichMapFunction<String, Tuple3<Long, String, Double>> {

        private static final long serialVersionUID = 1L;

        private String[] record;

        public OrderSplit() {
            record = new String[2];
        }

        @Override
        public Tuple3<Long, String, Double> map(String line) throws Exception {
            record = line.substring(1, line.length() - 1).split("\\|");
            //    return new Tuple3<>(Long.parseLong(record[0]), record[1], Integer.parseInt(record[2]));
            return new Tuple3<>(System.currentTimeMillis(), record[0], Double.parseDouble(record[3]));
        }
    }

    public static class LineitemSplit extends RichMapFunction<String, Tuple3<Long, String, Integer>> {

        private static final long serialVersionUID = 1L;

        private String[] record;

        public LineitemSplit() {
            record = new String[2];
        }

        @Override
        public Tuple3<Long, String, Integer> map(String line) throws Exception {
            record = line.substring(1, line.length() - 1).split("\\|");
            //    return new Tuple3<>(Long.parseLong(record[0]), record[1], Integer.parseInt(record[2]));
            return new Tuple3<>(System.currentTimeMillis(), record[0], Integer.parseInt(record[4]));
        }
    }

    private static class NameKeySelector1 implements KeySelector<Tuple3<Long, String, Double>, String> {
        @Override
        public String getKey(Tuple3<Long, String, Double> value) {
            return value.f1;
        }
    }
    private static class NameKeySelector2 implements KeySelector<Tuple3<Long, String, Integer>, String> {
        @Override
        public String getKey(Tuple3<Long, String, Integer> value) {
            return value.f1;
        }
    }

    private static class MyTimestampExtractor1 implements TimestampExtractor<Tuple3<Long, String, Double>> {
        private static final long serialVersionUID = 1L;

        @Override
        public long extractTimestamp(Tuple3<Long, String, Double> element, long currentTimestamp) {
            return element.f0;
        }

        @Override
        public long extractWatermark(Tuple3<Long, String, Double> element, long currentTimestamp) {
            return element.f0 - 1;
        }

        @Override
        public long getCurrentWatermark() {
            return Long.MIN_VALUE;
        }
    }

    private static class MyTimestampExtractor2 implements TimestampExtractor<Tuple3<Long, String, Integer>> {
        private static final long serialVersionUID = 1L;

        @Override
        public long extractTimestamp(Tuple3<Long, String, Integer> element, long currentTimestamp) {
            return element.f0;
        }

        @Override
        public long extractWatermark(Tuple3<Long, String, Integer> element, long currentTimestamp) {
            return element.f0 - 1;
        }

        @Override
        public long getCurrentWatermark() {
            return Long.MIN_VALUE;
        }
    }
    public static class MyJoinFunction
            implements
            JoinFunction<Tuple3<Long, String, Double>, Tuple3<Long, String, Integer>, Tuple3< String, Double, Integer>> {

        private static final long serialVersionUID = 1L;

        private Tuple3<String, Double, Integer> joined = new Tuple3<>();

        @Override
        public Tuple3<String, Double, Integer> join(Tuple3<Long, String, Double> first,
                                                     Tuple3<Long, String, Integer> second) throws Exception {
            joined.f0 = first.f1;
            joined.f1 = first.f2;
            joined.f2 = second.f2;
            return joined;
        }
    }
}
