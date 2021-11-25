package com.myslyv4uk.kafka.flink.streamoperations;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple8;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class FilterOrdersByDate implements FilterFunction<
				Tuple8<Integer,String, String, String, Integer, Double, String, Double>> {
	
	@Override
	public boolean filter(Tuple8<Integer, String, String, String, Integer, Double, String, Double> order)  {
		final LocalDate orderDate = LocalDate.parse(order.f3, DateTimeFormatter.ofPattern("yyyy/MM/d"));
		return orderDate.compareTo(LocalDate.of(2019, 11, 1)) >= 0 &&
						orderDate.compareTo(LocalDate.of(2019, 11, 11)) < 0;
	}
}
