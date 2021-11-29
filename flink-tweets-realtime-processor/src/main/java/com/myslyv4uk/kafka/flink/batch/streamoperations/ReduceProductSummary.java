package com.myslyv4uk.kafka.flink.batch.streamoperations;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;

public class ReduceProductSummary implements ReduceFunction <Tuple3<String,Integer,Double>> {
	
	@Override
	public Tuple3<String, Integer, Double> reduce(
					Tuple3<String, Integer, Double> rec1,
					Tuple3<String, Integer, Double> rec2
	)  {
		
		return new Tuple3<>(
						rec1.f0,           // Product name group
						rec1.f1 + rec2.f1, // Total Orders
						rec1.f2 + rec2.f2  // Total Order Value
		);
	}
}
