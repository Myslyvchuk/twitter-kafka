package com.myslyv4uk.kafka.flink.streamoperations;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.util.Collector;

public class FlatMapCustomerTag implements FlatMapFunction<
				Tuple7<Integer, String, String, String, Integer, Double, String>,
				Tuple2<String, String>> {
	@Override
	public void flatMap(Tuple7<Integer, String, String, String, Integer, Double, String> csvOrder,
											Collector<Tuple2<String, String>> collector) {
		
		//Extract customer and tags
		String customer = csvOrder.f1;
		String tags = csvOrder.f6;
		
		for(String tag : tags.split(":")) {
			collector.collect(new Tuple2<>(customer, tag));
		}
	}
}
