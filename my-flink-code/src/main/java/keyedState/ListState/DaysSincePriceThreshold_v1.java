package com.pluralsight.streaming;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class DaysSincePriceThreshold {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> inputData =
                env.readTextFile("src/main/resources/MSFT_2020.csv");

        DataStream<String> stockRecords = inputData.filter(
                (FilterFunction<String>) line ->
                        !line.contains("Date,Open,High,Low,Close,Adj Close,Volume,Name"));

        DataStream<Tuple3<String, String, Double>> closePrices = stockRecords.map(
                new MapFunction<String, Tuple3<String, String, Double>>() {
                    @Override
                    public Tuple3<String, String, Double> map(String s) throws Exception {
                        String[] tokens = s.split(",");

                        return new Tuple3<>(tokens[7], tokens[0], Double.parseDouble(tokens[5]));
                    }
                });

        closePrices.keyBy(value -> value.f0).flatMap(
                new NumberOfDaysSinceLastThresholdBreachFn()).print();

        env.execute();
    }

    public static class NumberOfDaysSinceLastThresholdBreachFn extends RichFlatMapFunction<
            Tuple3<String, String, Double>, Tuple2<String, Integer>> {

        private transient ListState<Double> pricesListState;

        @Override
        public void flatMap(Tuple3<String, String, Double> input,
                            Collector<Tuple2<String, Integer>> collector)
                throws Exception {

            if (input.f2 > 180) {

                Iterable<Double> prices = pricesListState.get();

                int count = 0;
                for (Double price: prices) {
                    count++;
                }

                pricesListState.clear();

                collector.collect(Tuple2.of(input.f1, count));
            } else {
                pricesListState.add(input.f2);
            }

        }

        @Override
        public void open(Configuration config) {

            ListStateDescriptor<Double> descriptor = new ListStateDescriptor<Double>(
                    "daysSinceThreshold", Double.class);

            pricesListState = getRuntimeContext().getListState(descriptor);
        }
    }
}