package com.myslyv4uk.kafka.flink.streamoperations;


import com.myslyv4uk.kafka.flink.model.ProductVendor;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple7;

public class OrderVendorJoinSelector implements JoinFunction
				<Tuple7<Integer, String, String, String, Integer, Double, String>,  //Input 1
								ProductVendor,                                              //Input 2
								Tuple2<String, Integer>> {                                  //Output
	@Override
	public Tuple2<String, Integer> join( //Output : Vendor, Item Count
					Tuple7<Integer, String, String, String, Integer, Double, String> order, //Input 1
					ProductVendor product) {//Input 2
		//Return Vendor and Item Count
		return new Tuple2<>(product.getVendor(), order.f4);
	}
}
