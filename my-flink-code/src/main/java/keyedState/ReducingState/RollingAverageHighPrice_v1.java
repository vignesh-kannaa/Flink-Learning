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

public class RollingAverageHighPrice {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> inputData =
                env.readTextFile("src/main/resources/MSFT_2020.csv");

        DataStream<String> stockRecords = inputData.filter(
                (FilterFunction<String>) line ->
                        !line.contains("Date,Open,High,Low,Close,Adj Close,Volume,Name"));

        DataStream<Tuple2<String, Double>> highPrices = stockRecords.map(
                new MapFunction<String, Tuple2<String, Double>>() {
                    @Override
                    public Tuple2<String, Double> map(String s) throws Exception {
                        String[] tokens = s.split(",");

                        return new Tuple2<>(tokens[7], Double.parseDouble(tokens[2]));
                    }
                });


        highPrices.keyBy(value -> value.f0).flatMap(
                new RollingAverageHighPriceFn()).print();

        env.execute();
    }

    public static class RollingAverageHighPriceFn extends RichFlatMapFunction<
            Tuple2<String, Double>, Tuple2<String, Double>> {

        private transient ValueState<Integer> countState;
        private transient ReducingState<Double> sumState;

        @Override
        public void flatMap(Tuple2<String, Double> input,
                            Collector<Tuple2<String, Double>> collector)
                throws Exception {

            Integer count = countState.value();

            if (count == null) {
                countState.update(1);
                sumState.add(input.f1);
            } else {

                if (count < 5) {
                    countState.update(count + 1);
                    sumState.add(input.f1);
                } else {
                    double average = sumState.get() / count;

                    countState.clear();
                    sumState.clear();

                    collector.collect(Tuple2.of(input.f0, average));
                }
            }

        }

        @Override
        public void open(Configuration config) {

            ValueStateDescriptor<Integer> valueStateDescriptor =
                    new ValueStateDescriptor<Integer>(
                            "CountPrice",
                            TypeInformation.of(
                                    new TypeHint<Integer>() {
                                    }));

            countState = getRuntimeContext().getState(valueStateDescriptor);

            ReducingStateDescriptor<Double> reducingStateDescriptor =
                    new ReducingStateDescriptor<Double>(
                            "SumPrice",
                            (ReduceFunction<Double>) (cumulative, input) -> cumulative + input,
                            Double.class);

            sumState = getRuntimeContext().getReducingState(reducingStateDescriptor);
        }
    }
}