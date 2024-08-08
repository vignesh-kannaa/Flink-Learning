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

public class MaxClosingPrice {

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
            new MaxClosingPriceFn()).print();

        env.execute();
    }

    public static class MaxClosingPriceFn extends RichFlatMapFunction<Tuple2<String, Double>, Double> {

        private transient ValueState<Double> maxClose;

        @Override
        public void flatMap(Tuple2<String, Double> input, Collector<Double> collector)
                throws Exception {

            Double maxClosePrice = maxClose.value();

            if (maxClosePrice == null) {
                maxClose.update(input.f1);
            } else {

                if (input.f1 > maxClosePrice) {
                    maxClose.update(input.f1);
                }
            }

            collector.collect(maxClose.value());
        }

        @Override
        public void open(Configuration config) {

            ValueStateDescriptor<Double> descriptor =
                    new ValueStateDescriptor<Double>(
                            "MaxPrice",
                            TypeInformation.of(
                                    new TypeHint<Double>() {
                                    }));

            maxClose = getRuntimeContext().getState(descriptor);
        }
    }
}