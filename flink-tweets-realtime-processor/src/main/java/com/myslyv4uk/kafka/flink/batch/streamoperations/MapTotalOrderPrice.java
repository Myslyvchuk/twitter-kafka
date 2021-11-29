package com.myslyv4uk.kafka.flink.batch.streamoperations;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.tuple.Tuple8;

public class MapTotalOrderPrice implements MapFunction<
				Tuple7<Integer, String, String, String, Integer, Double, String>,
				Tuple8<Integer, String, String, String, Integer, Double, String, Double>> {
	@Override
	public Tuple8<Integer, String, String, String, Integer, Double, String, Double> map(
					Tuple7<Integer, String, String, String, Integer, Double, String> csvOrder)  {
		return new Tuple8<>(
						csvOrder.f0,
						csvOrder.f1,
						csvOrder.f2,
						csvOrder.f3,
						csvOrder.f4,
						csvOrder.f5,
						csvOrder.f6,
						// compute total price
						(csvOrder.f4 * csvOrder.f5)
		);
	}
}
