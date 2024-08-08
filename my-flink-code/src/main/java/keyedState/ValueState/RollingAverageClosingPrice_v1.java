package com.pluralsight.streaming;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class RollingAverageClosingPrice {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> inputData =
                env.readTextFile("src/main/resources/MSFT_2020.csv");

        DataStream<String> stockRecords = inputData.filter(
                (FilterFunction<String>) line ->
                        !line.contains("Date,Open,High,Low,Close,Adj Close,Volume,Name"));

        DataStream<Tuple2<String, Double>> closePrices = stockRecords.map(
                new MapFunction<String, Tuple2<String, Double>>() {
                    @Override
                    public Tuple2<String, Double> map(String s) throws Exception {
                        String[] tokens = s.split(",");

                        return new Tuple2<>(tokens[7], Double.parseDouble(tokens[5]));
                    }
                });

        closePrices.keyBy(value -> value.f0).flatMap(
            new RollingAverageClosingPriceFn()).print();

        env.execute();
    }

    public static class RollingAverageClosingPriceFn extends RichFlatMapFunction<
            Tuple2<String, Double>, Tuple2<String, Double>> {

        private transient ValueState<Tuple2<Integer, Double>> rollingCountSum;

        @Override
        public void flatMap(Tuple2<String, Double> input,
                            Collector<Tuple2<String, Double>> collector)
                throws Exception {

            Tuple2<Integer, Double> countSum = rollingCountSum.value();

            if (countSum == null) {
                rollingCountSum.update(Tuple2.of(1, input.f1));
            } else {

                if (countSum.f0 < 5) {
                    rollingCountSum.update(Tuple2.of(countSum.f0 + 1, countSum.f1 + input.f1));
                } else {
                    double average = countSum.f1 / countSum.f0;

                    rollingCountSum.clear();

                    collector.collect(Tuple2.of(input.f0, average));
                }
            }
        }

        @Override
        public void open(Configuration config) {

            ValueStateDescriptor<Tuple2<Integer, Double>> descriptor =
                    new ValueStateDescriptor<Tuple2<Integer, Double>>(
                            "AvgPrice",
                            TypeInformation.of(
                                    new TypeHint<Tuple2<Integer, Double>>() {
                                    }));

            rollingCountSum = getRuntimeContext().getState(descriptor);
        }
    }
}